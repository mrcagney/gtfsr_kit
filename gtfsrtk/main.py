"""
This module does most of the work.

CONVENTIONS
============
- Unless specified otherwise, assume all GTFSr feeds are in the form of decoded JSON objects (Python dictionaries)
"""
import json
import time
from pathlib import Path

import requests
import pandas as pd
import numpy as np

import gtfsrtk.utilities as ut


def build_get_feed(url, headers, params):
    """
    Return a function that issues a GET request to the given 
    URL (string) with the given headers and parameters (dictionaries).

    Intended to be used to build a function that gets GTFSr feeds in 
    the form of decoded JSON objects.
    """
    def get_feed():
        r = requests.get(url, headers=headers, params=params)
        return r.json()

    return get_feed

def collect_feeds(get_feed, frequency, duration, out_dir, num_tries=3, 
  timestamp_format=ut.TIMESTAMP_FORMAT):
    """
    Assume the given function ``get_feed``, e.g. an output of 
    :func:`build_get_feed`, gets a GTFSr feed.
    Execute this function every ``frequency`` seconds for 
    a duration of ``duration`` seconds and store the resulting files as JSON 
    in the directory at ``out_dir``.
    Each file will be named '<timestamp>.json' where <timestamp>
    is the timestamp of the feed object retrieved, formatted 
    via the format string ``timestamp_format``.

    Try at most ``num_tries`` times in a row to get each trip updates object,
    and write nothing if that fails.

    The number of resulting trip updates will be at most 
    ``duration//frequency``.
    """
    out_dir = Path(out_dir)
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    num_calls = duration//frequency
    for i in range(num_calls):
        success = False
        n = 1
        while not success and n <= num_tries:
            try:
                feed = get_feed()
                success = True
                # Write to file
                t = get_timestamp(feed, timestamp_format)
                path = out_dir/'{0}.json'.format(t)
                with path.open('w') as tgt:
                    json.dump(feed, tgt)

            except requests.exceptions.RequestException as e:
                continue
            n += 1
        time.sleep(frequency)

def get_timestamp(feed, timestamp_format=ut.TIMESTAMP_FORMAT):
    """
    Given a GTFSr feed, return its timestamp in the given format.
    """
    return ut.format_timestamp(
      feed['response']['header']['timestamp'], 
      timestamp_format)

def extract_delays(feed, timestamp_format=ut.TIMESTAMP_FORMAT):
    """
    Given a GTFSr feed, extract delay data from its trip updates
    and return two things:

    1. A Pandas data frame with the columns:
        - route_id
        - trip_id
        - stop_id
        - stop_sequence
        - arrival_delay
        - departure_delay
    2. The timestamp of the update, which is a string of the given
      format

    If the feed has no trip updates, then data frame will be empty.
    """
    t = get_timestamp(feed, timestamp_format)
    rows = []
    for e in feed['response']['entity']:
        if 'trip_update' not in e:
            continue
        tu = e['trip_update']
        rid = tu['trip']['route_id']
        tid = tu['trip']['trip_id']
        stu = tu['stop_time_update']
        stop_sequence = int(stu['stop_sequence'])
        stop_id = str(stu['stop_id'])
        delay = {}
        for key in ['arrival', 'departure']:
            if key in stu:
                delay[key] = stu[key]['delay']
            else:
                delay[key] = np.nan
        rows.append((rid, tid, stop_sequence, stop_id, 
          delay['arrival'], delay['departure']))

    f = pd.DataFrame(rows, columns=[
      'route_id', 'trip_id', 'stop_sequence', 'stop_id',
      'arrival_delay', 'departure_delay'])
    f = f.sort_values(['route_id', 'trip_id', 'stop_sequence'])
    f.index = range(f.shape[0])
    return f, t

#@ut.time_it
def combine_delays(delays_list):
    """
    Given a list of delay data frames from roughly the same date
    (each the output of :func:`extract_delays`)
    combine them into a single data frame 
    and remove duplicate [route_id, trip_id, stop_sequence]
    entries by combining their non-null delay values
    into one entry.
    Return the resulting data frame.
    """
    f = pd.concat(delays_list)
    f = f.drop_duplicates()
    f = f.dropna(subset=['arrival_delay', 'departure_delay'],
      how='all')
    cols = ['route_id', 'trip_id', 'stop_sequence']
    f = f.sort_values(cols)
    # Backfill NaNs within each cols group.
    # Do this without groupby(), because that would create
    # too many groups.
    prev_tup = None
    new_rows = []
    dcols = ['arrival_delay', 'departure_delay']
    for __, row in f.iterrows():
        tup = row[cols].values 
        if np.array_equal(tup, prev_tup):
            # A duplicate route-trip-stop row.
            # Its arrival delay or departure delay is non-null
            # by virtue of our preprocessing.
            # Use its non-null delays to backfill the previous row's delays 
            # if necessary.
            if new_rows[-1][dcols].isnull().any():
                for dcol in dcols:
                    if pd.notnull(row[dcol]) and pd.isnull(new_rows[-1][dcol]):
                        new_rows[-1][dcol] = row[dcol]
        else:
            new_rows.append(row)
            prev_tup = tup
    f = pd.DataFrame(new_rows, index=range(len(new_rows)))
    return f

def clean_delays(delays, delay_cutoff=3600):
    """
    Given a delays data frame (output of :func:`extract_delays` or 
    :func:`combine_delays`),
    clean up the delays some and return the resulting data frame.

    Cleaning involves the following.

    1. Create a ``'delay'`` column that equals a trip's departure delay
      except at the final stop, in which case it equals the trip's 
      arrival delay.
    2. Drop the ``'departure_delay'`` and ``'arrival_delay'`` columns.
    3. Nullify fishy delays, that is, ones of at least ``delay_cutoff``
      seconds.
    """
    f = delays.copy()
        
    # Create a delay column
    def last_delay(group):
        group['delay'].iat[-1] = group['arrival_delay'].iat[-1]
        return group

    f['delay'] = f['departure_delay'].copy()
    f = f.groupby('trip_id').apply(last_delay)
    
    # Drop other delay columns
    f = f.drop(['arrival_delay', 'departure_delay'], axis=1)

    # Nullify fishy delays
    cond = abs(f['delay']) >= delay_cutoff
    f.loc[cond, 'delay'] = np.nan
        
    return f

def interpolate_delays(augmented_stop_times, delay_col, dist_threshold, num_decimals=0):
    """
    Given a data frame of GTFS stop times with an accurate ``'shape_dist_traveled'``
    column and extra column ``delay_col`` of trip delays,
    interpolate the delays of each trip as follows.
    If a trip has all null delays, then leave them as is.
    Otherwise:

    - If the first delay is more than ``dist_threshold`` distance units from the 
      first stop, then set the first stop delay to zero; otherwise
      set the first stop delay to the first delay.
    - If the last delay is more than ``dist_threshold`` distance units from the 
      last stop, then set the last stop delay to zero; otherwise 
      set the last stop delay to the last delay.
    - Linearly interpolate the remaining stop delays by distance.
    
    The distance unit is the one used in the ``'shape_dist_traveled'`` column.
    Return the resulting data frame.
    """
    f = augmented_stop_times.copy()
    if delay_col not in f.columns:
        return f

    def fill(group):
        # Don't fill trip with all null delays
        if group[delay_col].count() == 0:
            return group

        # Set first and last delays
        for i in [0, -1]:
            j = group[delay_col].dropna().index[i]
            dist_diff = abs(group['shape_dist_traveled'].iat[i] -\
              group['shape_dist_traveled'].ix[j])
            if dist_diff > dist_threshold:
                group[delay_col].iat[i] = 0
            else:
                group[delay_col].iat[i] = group[delay_col].ix[j]

        # Interpolate remaining delays
        ind = np.where(group[delay_col].notnull())[0]
        group[delay_col] = np.interp(group['shape_dist_traveled'], 
          group.iloc[ind]['shape_dist_traveled'], 
          group.iloc[ind][delay_col])

        return group 
    
    f = f.groupby('trip_id').apply(fill)
    # Round
    if num_decimals is not None:
        f[delay_col] = f[delay_col].round(num_decimals)
        
    return f
