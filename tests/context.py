from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.abspath('..'))

import gtfsrtk

# Load/create test feeds
DATA_DIR = Path('data')
GTFS_PATH = DATA_DIR/'test_gtfs.zip'
GTFSR_DIR = DATA_DIR/'test_gtfsr'
date = '20160519'
