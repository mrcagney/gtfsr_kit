from distutils.core import setup

setup(
    name='gtfsrtk',
    version='4.0.0',
    author='Alex Raichev',
    packages=['gtfsrtk', 'tests'],
    url='https://github.com/araichev/gtfsrtk',
    license='LICENSE',
    description='A Python 3.4 tool kit for processing General Transit Feed Specification Realtime (GTFSr) data',
    long_description=open('README.rst').read(),
    install_requires=[
        'gtfstk>=5.0.0',
        'pandas>=0.18.1, <0.19',
    ],
)

