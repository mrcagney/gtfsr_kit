import unittest
import shutil
import importlib
from pathlib import Path 
from types import FunctionType

import pandas as pd 
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_series_equal

from gtfsrtk.utilities import *
from gtfsrtk.main import *


# Load some feeds
DATA_DIR = Path('data')
GTFS_PATH = DATA_DIR/'test_gtfs.zip'
GTFSR_DIR = DATA_DIR/'test_gtfsr'
FEEDS = []
for path in GTFSR_DIR.iterdir():
    with path.open() as src:
        FEEDS.append(json.load(src))
DATE = '20160519'

class TestMain(unittest.TestCase):

    def test_build_get_feed(self):
        f = build_get_feed('bingo', {}, {})
        # Should be a function
        self.assertIsInstance(f, FunctionType)

    def test_get_timestamp(self):
        # Null feed should yield None
        self.assertEqual(get_timestamp(None), None)
        # Timestamp should be a string
        self.assertIsInstance(get_timestamp(FEEDS[0]), str)

    def test_extract_delays(self):
        for feed in [None, FEEDS[0]]:
            delays = extract_delays(feed)
            # Should be a data frame
            self.assertIsInstance(delays, pd.DataFrame)
            # Should have the correct columns
            expect_cols = ['route_id', 'trip_id', 'stop_id',
              'stop_sequence', 'arrival_delay', 'departure_delay']
            self.assertEqual(set(delays.columns), set(expect_cols))

    def test_combine_delays(self):
        delays_list = [extract_delays(f) for f in FEEDS]
        f = combine_delays(delays_list)
        # Should be a data frame
        self.assertIsInstance(f, pd.DataFrame)
        # Should have the correct columns
        expect_cols = ['route_id', 'trip_id', 'stop_id',
          'stop_sequence', 'arrival_delay', 'departure_delay']
        self.assertEqual(set(f.columns), set(expect_cols))

    def test_build_augmented_stop_times(self):
        gtfsr_feeds = []
        for f in GTFSR_DIR.iterdir():
            with f.open() as src:
                gtfsr_feeds.append(json.load(src))
        gtfs_feed = gt.read_gtfs(GTFS_PATH, dist_units_in='km')
        f = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, DATE)
        # Should be a data frame
        self.assertIsInstance(f, pd.DataFrame)
        # Should have the correct columns
        st = gt.get_stop_times(gtfs_feed, DATE)
        expect_cols = st.columns.tolist() + ['arrival_delay', 
          'departure_delay']
        self.assertEqual(set(f.columns), set(expect_cols))
        # Should have the correct number of rows
        self.assertEqual(f.shape[0], st.shape[0])

    def test_interpolate_delays(self):
        gtfsr_feeds = []
        for f in GTFSR_DIR.iterdir():
            with f.open() as src:
                gtfsr_feeds.append(json.load(src))
        gtfs_feed = gt.read_gtfs(GTFS_PATH, dist_units_in='km')
        ast = build_augmented_stop_times(gtfsr_feeds, gtfs_feed, DATE)
        f = interpolate_delays(ast, dist_threshold=1)
        # Should be a data frame
        self.assertIsInstance(f, pd.DataFrame)
        # Should have the correct columns
        self.assertEqual(set(f.columns), set(ast.columns))
        # Should have the correct number of rows
        self.assertEqual(f.shape[0], ast.shape[0])
        # For each trip, delays should be all nan or filled
        for __, group in f.groupby('trip_id'):
            n = group.shape[0]
            for col in ['arrival_delay', 'departure_delay']:
                k = group[col].count()
                self.assertTrue(k == 0 or k == n)


if __name__ == '__main__':
    unittest.main()