"""
CONVENTIONS
============
- Unless specified otherwise, assume all GTFSr feeds are in the form of Python dictionaries decoded from JSON objects
"""
import json
import time
from pathlib import Path

import requests
import pandas as pd
import numpy as np
import gtfstk as gt 

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
    If the feed is empty or ``None``, return ``None``.
    """
    if not feed:
        result = None
    else:
        result = ut.timestamp_to_str(
          feed['response']['header']['timestamp'], 
          timestamp_format)
    return result 

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

    If the feed has no trip updates, then the data frame will be empty.
    """
    t = get_timestamp(feed, timestamp_format)
    rows = []
    try:
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
    except (TypeError, KeyError):
        pass

    f = pd.DataFrame(rows, columns=[
      'route_id', 'trip_id', 'stop_sequence', 'stop_id',
      'arrival_delay', 'departure_delay'])
    f = f.sort_values(['route_id', 'trip_id', 'stop_sequence'])
    f.index = range(f.shape[0])
    return f, t

def combine_delays(delays_list):
    """
    Given a list of delay data frames from roughly the same date
    (each the output of :func:`extract_delays`),
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

@ut.time_it
def build_augmented_stop_times(gtfsr_path, gtfs_feed, date, 
  timestamp_format=ut.TIMESTAMP_FORMAT):
    """
    INPUTS:

    - ``gtfsr_path``: string or Path object; path to a directory that 
      contains GTFSr trip update feeds named <feed time stamp formatted>.json, 
      where the feed time stamp is formatted according to format
      ``timestamp_format``
    - ``gtfs_feed``: GTFSTK Feed instance corresponding to the GTFSr feeds 
    - ``date``: YYYYMMDD string
    - ``timestamp_format``: string or ``None``

    OUTPUT:

    - A data frame of GTFS stop times for trips scheduled on the given date 
      and containing two extra columns, ``'arrival_delay'`` and 
      ``'departure_delay'``, which are delay values in seconds 
      for that stop time according to the GTFSr feeds.  

    """
    gtfsr_path = Path(gtfsr_path)
    
    if not gtfsr_path.exists():
        raise ValueError('The GTFSr path {!s} does not exist'.format(
          gtfsr_path))

    # Get scheduled stop times for date
    st = gt.get_stop_times(gtfs_feed, date)
    
    # Get appropriate set of trip updates based on scheduled stop times
    start_time = '000000'
    start_datetime = date + start_time
    end_time = gt.timestr_to_seconds(st['departure_time'].max()) + 20*60 # Plus 20 minutes fuzz
    if end_time >= 24*3600:
        end_date = str(int(date) + 1)
    else:
        end_date = date
    end_time = gt.timestr_to_seconds(end_time, inverse=True)
    end_time = gt.timestr_mod24(end_time)
    end_time = end_time.replace(':', '')
    end_datetime = end_date + end_time
    
    # Extract delays
    delays_frames = []
    for f in gtfsr_path.iterdir():
        timestamp = ut.timestamp_to_str(f.stem, format=timestamp_format,
          inverse=True)
        datetime = ut.timestamp_to_str(timestamp, ut.TIMESTAMP_FORMAT) 
        if start_datetime <= datetime <= end_datetime:  
            with f.open() as src:
                tu = json.load(src)
                delays_frames.append(extract_delays(tu)[0])

    # Combine delays
    delays = combine_delays(delays_frames)     
    del delays['route_id']

    # Merge with stop times    
    ast = st.merge(delays, how='left', 
      on=['trip_id', 'stop_id', 'stop_sequence'])
  
    return ast.sort_values(['trip_id', 'stop_sequence'])

# TODO: Harmonize arrival and departure delays?
@ut.time_it
def fill_null_delays(augmented_stop_times, dist_threshold, 
  delay_threshold=3600):
    """
    INPUTS:

    - ``augmented_stop_times``: data frame; same format as output of  
      :func:`build_augmented_stop_times`
    - ``dist_threshold``: float; represents a distance in the same units
      as the ``'shape_dist_traveled'`` column of ``augmented_stop_times``,
      if that column is present

    OUTPUT:

    The data frame ``augmented_stop_times`` with delays altered as follows.
    Drop all delays with absolute value more than ``delay_threshold``seconds. 
    For each trip and for each delay type (arrival delay or departure delay)
    do the following. 
    If the trip has all null values for the delay type,
    then leave the values as is. 
    Otherwise:
    - If the first delay is more than ``dist_threshold`` distance units 
      from the first stop, then set the first stop delay to zero; otherwise
      set the first stop delay to the first delay.
    - If the last delay is more than ``dist_threshold`` distance units from 
      the last stop, then set the last stop delay to zero; otherwise 
      set the last stop delay to the last delay.
    - Linearly interpolate the remaining stop delays by distance.
    
    The distance unit is the one used in the ``'shape_dist_traveled'`` column.
    """
    f = augmented_stop_times.copy()

    # Return f if all the delays are present
    delay_cols = ['arrival_delay', 'departure_delay']
    if all([f[col].count() == f[col].shape[0] for col in delay_cols]):
        return f

    # Nullify fishy delays
    for col in delay_cols:
        f.loc[abs(f[col]) > delay_threshold, col] = np.nan

    # Fill null delays
    def fill(group):
        # Only columns that have at least one nonnull value.
        fill_cols = []
        for col in delay_cols:
            if group[col].count() >= 1:
                fill_cols.append(col)

        for col in fill_cols:
            # Set first and last delays
            for i in [0, -1]:
                j = group[col].dropna().index[i]
                dist_diff = abs(group['shape_dist_traveled'].iat[i] -\
                  group['shape_dist_traveled'].ix[j])
                if dist_diff > dist_threshold:
                    group[col].iat[i] = 0
                else:
                    group[col].iat[i] = group[col].ix[j]

            # Interpolate remaining delays
            ind = np.where(group[col].notnull())[0]
            group[col] = np.interp(group['shape_dist_traveled'], 
              group.iloc[ind]['shape_dist_traveled'], 
              group.iloc[ind][col])

            # Cast as int
            group[col] = group[col].astype(int)

        return group 
    
    f = f.groupby('trip_id').apply(fill)
        
    return f

def compute_trips_stats(gtfs_feed, augmented_stop_times):
    """
    """
    pass

def compute_routes_stats(gtfs_feed, augmented_stop_times):
    """
    INPUTS: 

    - ``gtfs_feed``: GTFSTK Feed instance
    - ``augmented_stop_times``: data frame; an output of 
      :func:`clean_augmented_stop_times` for some date; 
      corresponds to ``gtfs_feed``

    OUTPUTS:

    - A data frame with the columns
        * ``'route_id'`` 
        * ``'route_short_name'``
        * ``'route_type'``
        * ``'stop_delay_median'``
        * ``'stop_delay_iqr'``
        * ``'duration_delay_median'``
        * ``'duration_delay_iqr'``

    """
    feed = gtfs_feed
    ast = augmented_stop_times.copy()
    f = ast.merge(feed.trips['route_id', 'trip_id'])

    def stats(group):
        pass

    return f.groupby('route_id').apply(stats).reset_index()
