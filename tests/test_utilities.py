import unittest
import shutil
import importlib

import pandas as pd 
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_series_equal

from gtfsrtk.utilities import *


class TestUtilities(unittest.TestCase):

    def test_timestamp_to_str(self):
        t = 69.0
        for format in [None, TIMESTAMP_FORMAT]:
            s = timestamp_to_str(t)
            # Should be a string
            self.assertIsInstance(s, str)
            # Inverse should work
            tt = timestamp_to_str(s, inverse=True)
            self.assertEqual(t, tt)


if __name__ == '__main__':
    unittest.main()