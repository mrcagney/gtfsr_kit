from setuptools import setup, find_packages


# Import ``__version__`` variable
exec(open('gtfsrtk/_version.py').read())

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE.txt') as f:
    license = f.read()

setup(
    name='gtfsrtk',
    version=__version__,
    author='Alex Raichev',
    url='https://github.com/mrcagney/gtfsrtk',
    description='A Python 3.5+ tool kit for processing General Transit '
      'Feed Specification Realtime (GTFSR) data',
    long_description=readme,
    license=license,
    install_requires=[
        'gtfstk>=5.0.0',
        'pandas>=0.18.1',
    ],
    packages=find_packages(exclude=('tests', 'docs'))
 )
