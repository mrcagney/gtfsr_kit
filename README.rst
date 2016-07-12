GTFSrTK
********
A Python 3.4+ toolkit to process General Transit Feed Specification Realtime (GTFSr) data.
Uses Pandas to do the heavy lifting.


Installation
============
Create a Python 3.4+ virtual environment and ``pip install gtfsrtk``


Examples
========
You can play with ``ipynb/examples.ipynb`` in a Jupyter notebook


Documentation
==============
Documentation is in docs/ and also on RawGit `here <https://rawgit.com/araichev/gtfsrtk/master/docs/_build/singlehtml/index.html>`_.


Notes
======
- Development status is Alpha
- This project uses `semantic versioning <http://semver.org/>`_
- Only works on GTFSr feeds that are available in JSON format. I'll incorporate the protobuf format once `the Python 3 binding for protobuf gets fixed <https://github.com/google/gtfs-realtime-bindings/issues/17>`_.
- This project has been funded generously in part by `MRCagney <http://www.mrcagney.com/>`_


Authors
========
- Alex Raichev  (2016-06)


Changelog
==========

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