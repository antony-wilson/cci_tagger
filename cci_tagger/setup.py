import os

from setuptools import setup


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='cci_tagger',
    version='0.0.1',
    author=u'Antony Wilson',
    author_email='antony.wilson@stfc.ac.uk',
    include_package_data=True,
    url='http://stfc.ac.uk/',
    license='BSD licence',

    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],

    # Adds dependencies
    install_requires=[
        'SPARQLWrapper==1.7.6',
        'netCDF4==1.0.7',
    ],
)

