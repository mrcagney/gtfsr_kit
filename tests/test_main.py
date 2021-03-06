import pandas as pd
from google.transit.gtfs_realtime_pb2 import FeedMessage

from .context import gtfsr_kit, date, DATA_DIR, GTFSR_DIR, GTFS_PATH
from gtfsr_kit import *


path = DATA_DIR / "tripUpdates_short.json"
feed = read_feed(path, from_json=True)


def test_read_feed():
    path = DATA_DIR / "tripUpdates.pb"
    feed = read_feed(path)
    assert isinstance(feed, FeedMessage)

    path = DATA_DIR / "tripUpdates_short.json"
    feed = read_feed(path, from_json=True)
    assert isinstance(feed, FeedMessage)


def test_write_feed():
    paths = [DATA_DIR / "tripUpdates.pb", DATA_DIR / "tripUpdates_short.json"]
    json_flags = [False, True]
    for path, json_flag in zip(paths, json_flags):
        # Test round trip read-write-read
        path1 = path
        feed1 = read_feed(path1, from_json=json_flag)

        path2 = path1.parent / "tmp"
        write_feed(feed1, path2, to_json=json_flag)

        feed2 = read_feed(path2, from_json=json_flag)
        assert feed1 == feed2

        path2.unlink()


def test_feed_to_dict():
    d = feed_to_dict(feed)
    assert isinstance(d, dict)
    assert set(d.keys()) == {"header", "entity"}


def test_dict_to_feed():
    assert feed == dict_to_feed(feed_to_dict(feed))


def test_timestamp_to_str():
    t = 69
    for format in [None, "%Y%m%d%H%M%S"]:
        s = timestamp_to_str(t)
        # Should be a string
        assert isinstance(s, str)
        # Inverse should work
        tt = timestamp_to_str(s, inverse=True)
        assert t == tt


def test_get_timestamp_str():
    for f in [FeedMessage(), feed]:
        assert isinstance(get_timestamp_str(f), str)


def test_extract_delays():
    for f in [FeedMessage(), feed]:
        delays = extract_delays(f)

        # Should be a data frame
        assert isinstance(delays, pd.DataFrame)

        if f.header.timestamp:
            # Should have the correct columns
            expect_cols = [
                "route_id",
                "trip_id",
                "stop_id",
                "stop_sequence",
                "arrival_delay",
                "departure_delay",
            ]
            assert set(delays.columns) == set(expect_cols)
        else:
            # Should be empty
            assert delays.empty


def test_combine_delays():
    delays_list = [extract_delays(feed), extract_delays(feed)]
    f = combine_delays(delays_list)

    # Should be a data frame
    assert isinstance(f, pd.DataFrame)

    # Should have the correct columns
    expect_cols = [
        "route_id",
        "trip_id",
        "stop_id",
        "stop_sequence",
        "arrival_delay",
        "departure_delay",
    ]
    assert set(f.columns) == set(expect_cols)

    # Should eliminate duplicates
    assert pd.DataFrame.equals(f, extract_delays(feed))


def test_build_augmented_stop_times():
    gtfsr_feeds0 = []
    gtfsr_feeds1 = [read_feed(path, from_json=True) for path in GTFSR_DIR.iterdir()]
    gtfs_feed = gk.read_feed(GTFS_PATH, dist_units="km")

    for gtfsr_feeds in [gtfsr_feeds0, gtfsr_feeds1]:
        f = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date)

        # Should be a data frame
        assert isinstance(f, pd.DataFrame)

        # Should have the correct columns
        st = gk.get_stop_times(gtfs_feed, date)
        expect_cols = st.columns.tolist() + ["arrival_delay", "departure_delay"]
        assert set(f.columns) == set(expect_cols)

        # Should have the correct number of rows
        assert f.shape[0] == st.shape[0]


def test_interpolate_delays():
    gtfsr_feeds = [read_feed(path, from_json=True) for path in GTFSR_DIR.iterdir()]
    gtfs_feed = gk.read_feed(GTFS_PATH, dist_units="km")
    ast = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date)

    for delay_cols in [["arrival_delay"], ["arrival_delay", "departure_delay"]]:
        f = interpolate_delays(ast, dist_threshold=1, delay_cols=delay_cols)

        # Should be a data frame
        assert isinstance(f, pd.DataFrame)

        # Should have the correct columns
        assert set(f.columns) == set(ast.columns)

        # Should have the correct number of rows
        assert f.shape[0] == ast.shape[0]

        # For each trip, delays should be all nan or filled
        for __, group in f.groupby("trip_id"):
            n = group.shape[0]
            for col in delay_cols:
                k = group[col].count()
                assert k == 0 or k == n
