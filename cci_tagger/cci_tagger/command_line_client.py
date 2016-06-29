
import argparse
from datetime import datetime
import os
import sys
import time

from cci_tagger.mappings import LocalFacetMappings
from cci_tagger.tagger import ProcessDatasets

os.environ["DJANGO_SETTINGS_MODULE"] = "cedamoles_site.settings"
from cedamoles_app.models import Observation


def get_datasets_from_moles():
    """
    Get a list of datasets from MOLES which have a keyword of 'cci'.

    """
    internal_paths = set()
    obs = Observation.objects.filter(keywords__icontains='cci')
    for ob in obs:
        if has_dir_been_updated(ob.result.internalPath):
            internal_paths.add(ob.result.internalPath)
    return internal_paths


def has_dir_been_updated(path):
    """
    Check to see if the directory has been updated since we last checked.

    """
    # TODO
    # is dir mod date greater than X?
    return True


def get_datasets_from_file(file_name):
    """
    Get a list of datasets from the given file.

    @param file_name (str): the name of the file containing the list of
            datasets to process

    @return a List(str) of datasets

    """
    datasets = set()
    f = open(file_name, 'rb')
    for ds in f.readlines():
        datasets.add(ds.strip())
    return datasets


class CCITaggerCommandLineClient(object):

    def parse_command_line(self, argv):
        parser = argparse.ArgumentParser(
            description='Tag observations. You can tag an individual dataset, '
            'tag all the datasets listed in a file or all datasets known to '
            'MOLES. By default a check sum will be produces for each file.')

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            '-a', '--all', action='store_true',
            help='tag all datasets known to MOLES')
        group.add_argument(
            '-f', '--file',
            help=('the name of the file containing a list of datasets to '
                  'process. This option is used for tagging one or more '
                  'datasets.'))
        group.add_argument(
            '-d', '--dataset',
            help=('the full path to the dataset that is to be tagged. This '
                  'option is used to tag a single dataset.'))
        group.add_argument(
            '-s', '--show_mappings', action='store_true',
            help='show the mappings')

        parser.add_argument(
            '--file_count',
            help='how many .nt files to look at per dataset',
            type=int, default=0)
        parser.add_argument(
            '--no_check_sum', action='store_true',
            help='do not produce a check sum for each file')
        parser.add_argument(
            '-v', '--verbose', action='count',
            help='increase output verbosity',
            default=0)

        args = parser.parse_args(argv[1:])
        datasets = None
        if args.all:
            if args.verbose >= 1:
                print("\n%s STARTED" % (time.strftime("%H:%M:%S")))
                print("Processing all datasets in MOLES")
            datasets = get_datasets_from_moles()
        elif args.dataset is not None:
            if args.verbose >= 1:
                print("\n%s STARTED" % (time.strftime("%H:%M:%S")))
                print("Processing %s" % args.dataset)
            datasets = set([args.dataset])
        elif args.file is not None:
            if args.verbose >= 1:
                print("\n%s STARTED" % (time.strftime("%H:%M:%S")))
            datasets = get_datasets_from_file(args.file)
        elif args.show_mappings:
            print(LocalFacetMappings())

        return datasets, args

    @classmethod
    def main(cls, argv=sys.argv):
        start_time = datetime.now()
        client = cls()
        datasets, args = client.parse_command_line(argv)
        if datasets is None:
            sys.exit(0)

        pds = ProcessDatasets(
            checksum=not(args.no_check_sum), verbose=args.verbose)
        pds.process_datasets(datasets, args.file_count)

        if args.verbose >= 1:
            print("%s FINISHED\n\n" % (time.strftime("%H:%M:%S")))
            end_time = datetime.now()
            time_diff = end_time - start_time
            hours, remainder = divmod(time_diff.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            print('Time taken %02d:%02d:%02d' % (hours, minutes, seconds))

        exit(0)

if __name__ == "__main__":
    CCITaggerCommandLineClient.main()
