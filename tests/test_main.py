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


# Get a test feed
DATA_DIR = Path('data')
FEED_PATH = DATA_DIR/'auckland_gtfsr_trip_updates'/'20160520235543.json'
with FEED_PATH.open() as src:
    FEED = json.load(src)


class TestMain(unittest.TestCase):

    def test_build_get_feed(self):
        f = build_get_feed('bingo', {}, {})
        # Should be a function
        self.assertIsInstance(f, FunctionType)


    def test_get_timestamp(self):
        # Timestamp should match feed name
        t = get_timestamp(FEED)
        expect = FEED_PATH.stem
        self.assertEqual(t, expect)

        # Null feed should yield None
        self.assertEqual(get_timestamp(None), None)

    def test_extract_delays(self):
        for feed in [FEED, None]:
            delays, t = extract_delays(feed)
            # Types should be correct
            self.assertIsInstance(delays, pd.DataFrame)
            if feed is None:
                self.assertEqual(t, None)
            else:
                self.assertIsInstance(t, str)
            # delays should have the correct columns
            expect_cols = ['route_id', 'trip_id', 'stop_id',
              'stop_sequence', 'arrival_delay', 'departure_delay']
            self.assertEqual(set(delays.columns), set(expect_cols))


if __name__ == '__main__':
    unittest.main()