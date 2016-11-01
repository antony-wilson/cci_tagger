CCI Tagger
==========

Overview
--------

This package provides a command line tool moles_esgf_tag to generate dataset
tags for both MOLES and ESGF.


Installation
------------

It is recommended to run this in a Python virtual environment

Create a virtual environment::

  virtualenv tagger

Install the code::

  source tagger/bin/activate
  pip install cci_tagger-0.0.1.dev20.tar.gz

Usage
-----

moles_esgf_tag [-h] (-f FILE | -d DATASET | -s) [--file_count FILE_COUNT] [--no_check_sum] [-v]

Tag observations. You can tag an individual dataset, or tag all the datasets
listed in a file. By default a check sum will be produces for each file.

Arguments::

  -h, --help            show this help message and exit

  -f FILE, --file FILE  the name of the file containing a list of datasets to
                        process. This option is used for tagging one or more
                        datasets.

  -d DATASET, --dataset DATASET
                        the full path to the dataset that is to be tagged.
                        This option is used to tag a single dataset.

  -s, --show_mappings   show the local vocabulary mappings

  -m, --use_mappings    use the local vocabulary mappings

  --file_count FILE_COUNT
                        how many .nt files to look at per dataset

  --no_check_sum        do not produce a check sum for each file

  -v, --verbose         increase output verbosity

A number of files are produced as output:
  esgf_drs.json contains a list of DRS and associated files and check sums
  
  moles_tags.csv contains a list of dataset paths and vocabulary URLs
  
  moles_esgf_mapping.csv contains mappings between dataset paths and DRS
  
  error.txt contains a list of errors
  
Examples:
  moles_esgf_tag -d /neodc/esacci/cloud/data/L3C/avhrr_noaa-16 -v
  
  moles_esgf_tag -f datapath --file_count 2 --no_check_sum -v
  
  moles_esgf_tag -s
