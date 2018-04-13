import pandas as pd

from .context import gtfsrtk, feeds, date, DATA_DIR, GTFSR_DIR, GTFS_PATH
from gtfsrtk import *


def test_pb_to_json():
    path = DATA_DIR/'tripUpdates.pb'
    feed = gtfs_realtime_pb2.FeedMessage()
    with path.open('rb') as src:
        feed.ParseFromString(src.read())

    feed_j = pb_to_json(feed)
    assert isinstance(feed_j, dict)
    assert set(feed_j.keys()) == {'header', 'entity'}

def test_read_gtfsr():
    path = DATA_DIR/'tripUpdates.pb'
    feed = read_gtfsr(path)
    assert isinstance(feed, dict)
    assert set(feed.keys()) == {'header', 'entity'}

    feed = read_gtfsr(path, as_pb=True)
    assert isinstance(feed, gtfs_realtime_pb2.FeedMessage)

def test_timestamp_to_str():
    t = 69.0
    for format in [None, TIMESTAMP_FORMAT]:
        s = timestamp_to_str(t)
        # Should be a string
        assert isinstance(s, str)
        # Inverse should work
        tt = timestamp_to_str(s, inverse=True)
        assert t == tt

def test_get_timestamp():
    # Null feed should yield None
    assert get_timestamp(None) is None
    # Timestamp should be a string
    assert isinstance(get_timestamp(feeds[0]), str)

def test_extract_delays():
    for feed in [None, feeds[0]]:
        delays = extract_delays(feed)
        # Should be a data frame
        assert isinstance(delays, pd.DataFrame)
        # Should have the correct columns
        expect_cols = ['route_id', 'trip_id', 'stop_id',
          'stop_sequence', 'arrival_delay', 'departure_delay']
        assert set(delays.columns) == set(expect_cols)

def test_combine_delays():
    delays_list = [extract_delays(f) for f in feeds]
    f = combine_delays(delays_list)
    # Should be a data frame
    assert isinstance(f, pd.DataFrame)
    # Should have the correct columns
    expect_cols = ['route_id', 'trip_id', 'stop_id',
      'stop_sequence', 'arrival_delay', 'departure_delay']
    assert set(f.columns) == set(expect_cols)

def test_build_augmented_stop_times():
    gtfsr_feeds = []
    for f in GTFSR_DIR.iterdir():
        with f.open() as src:
            gtfsr_feeds.append(json.load(src))
    gtfs_feed = gt.read_gtfs(GTFS_PATH, dist_units='km')
    f = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date)
    # Should be a data frame
    assert isinstance(f, pd.DataFrame)
    # Should have the correct columns
    st = gt.get_stop_times(gtfs_feed, date)
    expect_cols = st.columns.tolist() + ['arrival_delay',
      'departure_delay']
    assert set(f.columns) == set(expect_cols)
    # Should have the correct number of rows
    assert f.shape[0] == st.shape[0]

def test_interpolate_delays():
    gtfsr_feeds = []
    for f in GTFSR_DIR.iterdir():
        with f.open() as src:
            gtfsr_feeds.append(json.load(src))
    gtfs_feed = gt.read_gtfs(GTFS_PATH, dist_units='km')
    ast = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date)
    f = interpolate_delays(ast, dist_threshold=1)
    # Should be a data frame
    assert isinstance(f, pd.DataFrame)
    # Should have the correct columns
    assert set(f.columns) == set(ast.columns)
    # Should have the correct number of rows
    assert f.shape[0] == ast.shape[0]
    # For each trip, delays should be all nan or filled
    for __, group in f.groupby('trip_id'):
        n = group.shape[0]
        for col in ['arrival_delay', 'departure_delay']:
            k = group[col].count()
            assert k == 0 or k == n