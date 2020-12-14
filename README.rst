GTFSR Kit
*********
.. image:: https://travis-ci.org/mrcagney/gtfrstk.svg?branch=master
    :target: https://travis-ci.org/mrcagney/gtfsrtk

GTFSR Kit is a tiny Python 3.8+ library to process `General Transit Feed Specification Realtime (GTFSR) data <https://developers.google.com/transit/gtfs-realtime/reference>`_.
It does some simple things like read and write Protocol Buffer or JSON feed files.
It also does some complex things like extract and combine delays into Pandas DataFrames.


Installation
============
Do ``poetry add gtfsr_kit``.


Examples
========
See the Jupyter notebook at ``notebooks/examples.ipynb``.


Documentation
==============
Documentation is in docs/ and also on RawGit `here <https://rawgit.com/araichev/gtfsr_kit/master/docs/_build/singlehtml/index.html>`_.


Notes
======
- Development status is Alpha
- This project uses `semantic versioning <http://semver.org/>`_
- Thanks to `MRCagney <http://www.mrcagney.com/>`_ for funding this project


Authors
========
- Alex Raichev  (2016-06)


Changelog
==========

6.2.0, 2020-12-??
-----------------
- Upgraded to Python 3.8.


6.1.0, 2018-07-11
------------------
- Added ``delay_cols`` keyword argument to ``interpolate_delays``


6.0.2, 2018-04-18
------------------
- Handled edge case in ``build_augmented_stop_times`` for real this time!


6.0.1, 2018-04-18
------------------
- Handled edge case in ``build_augmented_stop_times``


6.0.0, 2018-04-18
------------------
- Renamed function ``dictify`` to ``feed_to_dict`` and added the inverse function ``dict_to_feed``


5.0.1, 2018-04-17
------------------
- Fixed setup.py


5.0.0, 2018-04-17
------------------
- Finally handled Protocol Buffer feed files, thanks to version 0.0.5 of `the Python gtfs-realtime-bindings <https://github.com/google/gtfs-realtime-bindings/tree/master/python>`_
- Switched to using Google FeedMessage objects natively
- Simplified code


4.0.0, 2016-07-13
------------------
- Changed the signature of ``main.collect_feeds``


3.0.1, 2016-07-12
------------------
- Bugfixed ``ipynb/examples.ipynb``


3.0.0, 2016-07-12
------------------
- Changed to a small data set for tests
- Upgraded to gtfstk==5.0.0
- Removed ``time_it`` decorators from functions
- Changed signature of ``main.build_augmented_stop_times`` to be more versatile


2.0.0, 2016-07-08
------------------
- Refactored heaps
- Added automated tests
- Added Sphinx docs
- Uploaded to pip


1.0.0, 2016-06-12
------------------
- First release