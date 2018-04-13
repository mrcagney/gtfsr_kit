"""
Throughout this code a *GTFSR feed* is a Python dictionary version
of a Protocol Buffer GTFSR feed.
In particular, the top level keys of the feed are
``'header'`` and ``'entity'``.
The dictionary versions are bigger but, in my opinion, easier to
reason about.
"""
import datetime as dt
import json
from pathlib import Path

import pandas as pd
import numpy as np
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2
import gtfstk as gt


TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

def pb_to_json(pb_feed):
    """
    Convert a Protocol Buffer GTFSR feed into a dictionary GTFSR feed.
    """
    return json.loads(json_format.MessageToJson(pb_feed))

def read_gtfsr(path, *, from_json=False):
    path = Path(path)
    feed = gtfs_realtime_pb2.FeedMessage()

    with path.open('rb') as src:
        if from_json:
            feed = json_format.Parse(src.read(), feed)
        else:
            feed.ParseFromString(src.read())

    return feed

def timestamp_to_str(t, format=TIMESTAMP_FORMAT, *, inverse=False):
    """
    Given a POSIX timestamp (float) ``t``, format it as a string
    in the given format.
    If ``inverse``, then do the inverse, that is, assume ``t`` is
    a string in the given format and return its corresponding timestamp.
    If ``format is None``, then cast ``t`` as a float (if not ``inverse``)
    or string (if ``inverse``) directly.
    """
    if not inverse:
        if format is None:
            result = str(t)
        else:
            result = dt.datetime.fromtimestamp(t).strftime(format)
    else:
        if format is None:
            result = float(t)
        else:
            result = dt.datetime.strptime(t, format).timestamp()
    return result

def get_timestamp(feed, timestamp_format=TIMESTAMP_FORMAT):
    """
    INPUTS:

    - ``feed``: GTFSR feed
    - ``timestamp_format``: string; specifies how to format timestamps

    OUTPUTS:

    The timestamp of the feed formatted according to ``timestamp_format``.
    If the feed is empty or ``None``, return ``None``.
    """
    if not feed:
        result = None
    elif isinstance(feed, gtfs_realtime_pb2.FeedMessage):
        result = timestamp_to_str(feed.header.timestamp,
          timestamp_format)
    else:
        result = timestamp_to_str(feed['header']['timestamp'],
          timestamp_format)
    return result

def extract_delays(feed, timestamp_format=TIMESTAMP_FORMAT):
    """
    INPUTS:

    - ``feed``: GTFSR feed
    - ``timestamp_format``: string; specifies how to format timestamps

    OUTPUTS:

    A Pandas data frame built from the feed with the columns:

    - route_id
    - trip_id
    - stop_id
    - stop_sequence
    - arrival_delay
    - departure_delay

    If the feed has no trip updates, then the data frame will be empty.
    """
    rows = []
    if isinstance(feed, gtfs_realtime_pb2.FeedMessage):
        for e in feed.entity:
            if not e.HasField('trip_update'):
                continue
            tu = e.trip_update
            rid = tu.trip.route_id
            tid = tu.trip.trip_id
            stu = tu.stop_time_update
            for x in stu:
                stop_sequence = int(x.stop_sequence)
                stop_id = str(x.stop_id)
                delay = {}
                for key in ['arrival', 'departure']:
                    if x.HasField(key):
                        delay[key] = getattr(x, key).delay
                    else:
                        delay[key] = np.nan
                rows.append((rid, tid, stop_sequence, stop_id,
                  delay['arrival'], delay['departure']))

    f = pd.DataFrame(rows, columns=[
      'route_id', 'trip_id', 'stop_sequence', 'stop_id',
      'arrival_delay', 'departure_delay'])
    f = f.sort_values(['route_id', 'trip_id', 'stop_sequence'])
    f.index = range(f.shape[0])
    return f

def combine_delays(delays_list):
    """
    INPUTS:

    - ``delays_list``: nonempty list of delay data frames, each of the form
      output by :func:`extract_delays`

    OUTPUTS:

    A data frame of the same format as each data frame in ``delays_list``
    obtained as follows
    Concatenate ``delays_list`` and remove duplicate
    [route_id, trip_id, stop_sequence]
    entries by combining their non-null delay values
    into one entry.
    Warning: for a sensible output, don't include in ``delays_list`` the
    same trips on different days.
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

def build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date):
    """
    INPUTS:

    - ``gtfsr_feeds``: list; GTFSR feeds
    - ``gtfs_feed``: GTFSTK Feed instance corresponding to the GTFSR feeds
    - ``date``: YYYYMMDD string
    - ``timestamp_format``: string or ``None``

    OUTPUT:

    - A data frame of GTFS stop times for trips scheduled on the given date
      and containing two extra columns, ``'arrival_delay'`` and
      ``'departure_delay'``, which are delay values in seconds
      for that stop time according to the GTFSR feeds given.

    """
    # Get scheduled stop times for date
    st = gt.get_stop_times(gtfs_feed, date)

    # Get GTFSR timestamps pertinent to date
    start_time = '000000'
    start_datetime = date + start_time
    # Plus 20 minutes fuzz:
    end_time = gt.timestr_to_seconds(st['departure_time'].max()) + 20*60
    if end_time >= 24*3600:
        end_date = str(int(date) + 1)
    else:
        end_date = date
    end_time = gt.timestr_to_seconds(end_time, inverse=True)
    end_time = gt.timestr_mod24(end_time)
    end_time = end_time.replace(':', '')
    end_datetime = end_date + end_time

    # Extract delays
    delays_frames = [extract_delays(f) for f in gtfsr_feeds
      if start_datetime <= get_timestamp(f) <= end_datetime]

    # Combine delays
    delays = combine_delays(delays_frames)
    del delays['route_id']

    # Merge with stop times
    ast = st.merge(delays, how='left',
      on=['trip_id', 'stop_id', 'stop_sequence'])

    return ast.sort_values(['trip_id', 'stop_sequence'])

def interpolate_delays(augmented_stop_times, dist_threshold,
  delay_threshold=3600):
    """
    INPUTS:

    - ``augmented_stop_times``: data frame; same format as output of
      :func:`build_augmented_stop_times`
    - ``dist_threshold``: float; a distance in the same units
      as the ``'shape_dist_traveled'`` column of ``augmented_stop_times``,
      if that column is present
    - ``delay_threshold``: integer; number of seconds

    OUTPUT:

    The data frame ``augmented_stop_times`` with delays altered as follows.
    Drop all delays with absolute value more than ``delay_threshold`` seconds.
    For each trip and for each delay type (arrival delay or departure delay)
    do the following.
    If the trip has all null values for the delay type,
    then leave the values as is.
    Otherwise:

    - If the first delay is more than ``dist_threshold`` distance units
      from the first stop, then set the first stop delay to zero (charitably);
      otherwise set the first stop delay to the first delay.
    - If the last delay is more than ``dist_threshold`` distance units from
      the last stop, then set the last stop delay to zero (charitably);
      otherwise set the last stop delay to the last delay.
    - Linearly interpolate the remaining stop delays by distance.
    """
    f = augmented_stop_times.copy()

    # Return f if nothing to do
    delay_cols = ['arrival_delay', 'departure_delay']
    if 'shape_dist_traveled' not in f.columns or\
      not f['shape_dist_traveled'].notnull().any() or\
      all([f[col].count() == f[col].shape[0] for col in delay_cols]):
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
                dist_diff = (abs(group['shape_dist_traveled'].iat[i] -
                  group['shape_dist_traveled'].ix[j]))
                if dist_diff > dist_threshold:
                    group[col].iat[i] = 0
                else:
                    group[col].iat[i] = group[col].ix[j]

            # Interpolate remaining delays
            ind = np.where(group[col].notnull())[0]
            group[col] = np.interp(group['shape_dist_traveled'],
              group.iloc[ind]['shape_dist_traveled'],
              group.iloc[ind][col])

        return group

    f = f.groupby('trip_id').apply(fill)

    # Round
    f[delay_cols] = f[delay_cols].round(0)

    return f