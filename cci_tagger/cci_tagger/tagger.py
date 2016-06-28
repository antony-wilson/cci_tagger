'''
BSD Licence
Copyright (c) 2016, Science & Technology Facilities Council (STFC)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
        this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
        this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.
    * Neither the name of the Science & Technology Facilities Council (STFC)
        nor the names of its contributors may be used to endorse or promote
        products derived from this software without specific prior written
        permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''

import hashlib
import json
import os

from cci_tagger.constants import DATA_TYPE, FREQUENCY, INSTITUTION, PLATFORM,\
    SENSOR, ECV, PLATFORM_PROGRAMME, PLATFORM_GROUP, PROCESSING_LEVEL,\
    PRODUCT_STRING, PRODUCT_VERSION
from cci_tagger.facets import Facets
from cci_tagger.mappings import DRS_Mapping, LocalFacetMappings
from cci_tagger.triple_store import TripleStore
import netCDF4


class ProcessDatasets(object):
    """
    This class provides the process_datasets method to process datasets,
    extract data from file names and from within net cdf files. It then
    produces 2 files, one for input into MOLES and ont for input into ESGF

    """
    ESACCI = 'ESACCI'
    DRS_ESACCI = 'esacci'
    MULTI_SENSOR = 'multi-sensor'
    MULTI_PLATFORM = 'multi-platform'

    # an instance of the facets class
    __facets = None

    __allowed_net_cdf_attribs = [FREQUENCY, INSTITUTION, PLATFORM, SENSOR]

    def __init__(self, checksum=True, use_mapping=True, verbose=False):
        """
        Initialise the ProcessDatasets class.

        @param checksum (boolean): if True produce a checksum for each file
        @param use_mapping (boolean): if True use the local mapping to correct
                use values to match those in the vocab server
        @param verbose (boolean): if True produce additional output

        """
        self.__checksum = checksum
        self.__use_mapping = use_mapping
        self.__verbose = verbose
        if self.__facets is None:
            self.__facets = Facets()
        self.__drs_mapping = DRS_Mapping()
        self.__file_drs = None
        self.__file_csv = None
        self.__file_error = None
        self.__file_drs_ids = None
        self._open_files()
        self.__not_found_messages = set()
        self.__error_messages = set()
        self.__drs_messages = set()

    def process_datasets(self, datasets, max_file_count):
        """
        Loop through the datasets pulling out data from file names and from
        within net cdf files.

        @param datasets (List(str)): a list of dataset names, these are the
        full paths to the datasets
        @param max_file_count (int): how many .nt files to look at per dataset.
                If the value is less than 1 then all datasets will be
                processed.

        """
        ds_len = len(datasets)
        if max_file_count > 0:
            print("Processing a maximum of %s files for each of %s datasets" %
                  (max_file_count, ds_len))
        else:
            print("Processing %s datasets" % ds_len)
        drs = {}
        count = 0

        for ds in sorted(datasets):
            count = count + 1
            self._process_dataset(ds, count, drs, max_file_count)

        self._write_json(drs)

        if len(self.__not_found_messages) > 0:
            print("\nSUMMARY OF TERMS NOT IN THE VOCAB:\n")
            for message in sorted(self.__not_found_messages):
                print(message)

        for message in sorted(self.__error_messages):
            self.__file_error.write('%s\n' % message)

        self.__drs_mapping.output_drs(self.__drs_messages)

        self._close_files()

    def _process_dataset(self, ds, count, drs, max_file_count):
        """
        Pull out data from file names and from within net cdf files.

        @param ds (str): the full path to the dataset
        @param count (int): the sequence number for this dataset
        @param drs {dict}: key (str) = DRS label
                           value (dict):
                               key = 'file', value = the file path
                               key = 'sha256', value = the sha256 of the file
        @param max_file_count (int): how many .nt files to look at per dataset
                If the value is less than 1 then all datasets will be
                processed.

        """
        tags_ds = {}
        drs_count = 0

        # key drs id, value realization
        current_drs_ids = self._get_drs_ids(ds)

        # get a list of files
        nc_files = self._get_nc_files(ds, max_file_count)

        print("\nDataset %s Processing %s files from %s" %
              (count, len(nc_files), ds))

        if len(nc_files) == 0:
            self.__error_messages.add('WARNING %s, no .nc files found' % (ds))
            return

        for fpath in nc_files:
            drs_ds = {}

            net_cdf_drs, net_cdf_tags = self._parse_file_name(
                ds, fpath)
            drs_ds.update(net_cdf_drs)
            tags_ds.update(net_cdf_tags)
            net_cdf_drs, net_cdf_tags = self._scan_net_cdf_file(
                fpath, ds)
            drs_ds.update(net_cdf_drs)
            tags_ds.update(net_cdf_tags)

            dataset_id = self._generate_ds_id(ds, drs_ds)
            # only add files with all of the drs data
            if dataset_id is None:
                continue

            if dataset_id not in current_drs_ids.keys():
                current_drs_ids[dataset_id] = self._get_next_realization(
                    ds, dataset_id, drs)
                dataset_id = '%s.%s' % (
                    dataset_id, current_drs_ids[dataset_id])
                self.__drs_messages.add('%s,%s' % (ds, dataset_id))
            else:
                dataset_id = '%s.%s' % (
                    dataset_id, current_drs_ids[dataset_id])

            if self.__checksum:
                sha256 = self._sha256(fpath)
                if dataset_id in drs.keys():
                    drs[dataset_id].append({'file': fpath, 'sha256': sha256})
                else:
                    drs_count = drs_count + 1
                    drs[dataset_id] = [{'file': fpath, 'sha256': sha256}]
            else:
                if dataset_id in drs.keys():
                    drs[dataset_id].append({'file': fpath})
                else:
                    drs_count = drs_count + 1
                    drs[dataset_id] = [{'file': fpath}]
                    print('DRS = %s' % dataset_id)

        if drs_count == 0:
            self.__error_messages.add(
                'ERROR in %s, no DRS entries created' % (ds))

        print("Created {count} DRS {entry}".format(
            count=drs_count, entry='entry' if drs_count == 1 else 'entries'))

        self._write_csv(ds, tags_ds)

    def _get_drs_ids(self, ds):
        # TODO Search a file? to get
        # {internal_path:{drs:realization}
        # {internal_path:drs}
        return {}

    def _sha256(self, fpath):
        """
        Generate the sha256 for the given file.

        @param (str): the path to the file

        @return the sha256 of the file

        """
        h = hashlib.sha256()
        f = open(fpath)
        while True:
            data = f.read(10240)
            if not data:
                break
            h.update(data)
        f.close()
        return h.hexdigest()

    def _get_nc_files(self, dir_, max_file_count):
        """
        Get the list of net cdf files in the given directory.

        @param dir_ (str): the name of the directory to scan
        @param max_file_count (int): how many .nt files to look at per dataset
                If the value is less than 1 then all datasets will be
                processed.

        @return a list of file names complete with paths

        """
        file_list = []
        count = 1
        for root, _, files in os.walk(dir_):
            for name in files:
                if name.endswith('.nc'):
                    file_list.append(os.path.join(root, name))
                    count = count + 1
                    if max_file_count > 0 and count > max_file_count:
                        return file_list
        return file_list

    def _parse_file_name(self, ds, fpath):
        """
        Extract data from the file name.

        The file name comes in two different formats. The values are '-'
        delimited.
        Form 1
            <Indicative Date>[<Indicative Time>]-ESACCI
            -<Processing Level>_<CCI Project>-<Data Type>-<Product String>
            [-<Additional Segregator>][-v<GDS version>]-fv<File version>.nc
        Form 2
            ESACCI-<CCI Project>-<Processing Level>-<Data Type>-
            <Product String>[-<Additional Segregator>]-
            <IndicativeDate>[<Indicative Time>]-fv<File version>.nc

        Values extracted from the file name:
            Processing Level - level
            CCI Project - ecv_id
            Data Type - variable
            Product String - product_id

        @param ds (str): the full path to the dataset
        @param fpath (str): the path to the file

        @return drs and csv representations of the data

        """

        path_facet_bits = fpath.split('/')
        last_bit = len(path_facet_bits) - 1
        file_segments = path_facet_bits[last_bit].split('-')
        if len(file_segments) < 5:
            message_found = False
            # Do not add another message if we have already reported an invalid
            # file name for this dataset
            for message in self.__error_messages:
                if (message.startswith('ERROR in %s, invalid file name format'
                                       % (ds))):
                    message_found = True
            if not message_found:
                self.__error_messages.add(
                    'ERROR in %s, invalid file name format "%s"' %
                    (ds, path_facet_bits[last_bit]))
            return {}, {}

        if file_segments[1] == self.ESACCI:
            return self._process_form(
                ds, self._get_data_from_file_name_1(file_segments))
        elif file_segments[0] == self.ESACCI:
            return self._process_form(
                ds, self._get_data_from_file_name_2(file_segments))
        else:
            message_found = False
            # Do not add another message if we have already reported an invalid
            # file name for this dataset
            for message in self.__error_messages:
                if (message.startswith('ERROR in %s, invalid file name format'
                                       % (ds))):
                    message_found = True
            if not message_found:
                self.__error_messages.add(
                    'ERROR in %s, invalid file name format "%s"' %
                    (ds, path_facet_bits[last_bit]))
            return {}, {}

    def _get_data_from_file_name_1(self, file_segments):
        """
        Extract data from the file name of form 1.

        @param file_segments (List(str)): file segments

        @return a dict where:
                key = facet name
                value = file segment

        """
        form = {}
        form[PROCESSING_LEVEL] = file_segments[2].split('_')[0]
        form[ECV] = file_segments[2].split('_')[1]
        form[DATA_TYPE] = file_segments[3]
        form[PRODUCT_STRING] = file_segments[4]
        return form

    def _get_data_from_file_name_2(self, file_segments):
        """
        Extract data from the file name of form 2.

        @param file_segments (List(str)): file segments

        @return a dict where:
                key = facet name
                value = file segment

        """
        form = {}
        form[PROCESSING_LEVEL] = file_segments[2]
        form[ECV] = file_segments[1]
        form[DATA_TYPE] = file_segments[3]
        form[PRODUCT_STRING] = file_segments[4]
        return form

    def _process_form(self, ds, form):
        """
        Process form to generate drs and csv representations.

        @param ds (str): the full path to the dataset
        @param form (dict): data extracted from the file name

        @return drs and csv representations of the data

        """
        csv_rec = {}
        term = self._get_term_uri(
            PROCESSING_LEVEL, form[PROCESSING_LEVEL], ds)
        if term is not None:
            csv_rec[PROCESSING_LEVEL] = term

        term = self._get_term_uri(
            ECV, form[ECV], ds)
        if term is not None:
            csv_rec[ECV] = term

        term = self._get_term_uri(
            DATA_TYPE, form[DATA_TYPE], ds)
        if term is not None:
            csv_rec[DATA_TYPE] = term

        term = self._get_term_uri(
            PRODUCT_STRING, form[PRODUCT_STRING], ds)
        if term is not None:
            csv_rec[PRODUCT_STRING] = term
        return self._create_drs_record(csv_rec), csv_rec

    def _create_drs_record(self, csv_rec):
        proc_lev_label = TripleStore.get_alt_label(
            csv_rec.get(PROCESSING_LEVEL))
        project_label = TripleStore.get_alt_label(csv_rec.get(ECV))
        data_type_label = TripleStore.get_alt_label(csv_rec.get(DATA_TYPE))
        product_label = TripleStore.get_pref_label(csv_rec.get(PRODUCT_STRING))
        drs = {}
        if project_label != '':
            drs[ECV] = project_label
        if proc_lev_label != '':
            drs[PROCESSING_LEVEL] = proc_lev_label
        if data_type_label != '':
            drs[DATA_TYPE] = data_type_label
        if product_label != '':
            drs[PRODUCT_STRING] = product_label
        return drs

    def _scan_net_cdf_file(self, fpath, ds):
        """
        Extract data from the net cdf file.

        The values to extract are take from the know_attr list which are the
        keys of the attr_mapping dictionary.

        """
        drs = {}
        tags = {}
        try:
            nc = netCDF4.Dataset(fpath)
        except:
            self.__error_messages.add(
                'ERROR in %s, extracting attributes from "%s"' % (ds, fpath))
            return drs, tags

        if self.__verbose:
            print("GLOBAL ATTRS for %s: " % fpath)
        for global_attr in nc.ncattrs():
            if self.__verbose:
                print(global_attr, "=", nc.getncattr(global_attr))

            if global_attr.lower() in self.__allowed_net_cdf_attribs:
                attr = nc.getncattr(global_attr)
                a_drs, a_tags = self._process_file_atrib(
                    global_attr.lower(), attr, ds)
                drs.update(a_drs)
                tags.update(a_tags)
            # we don't have a vocab for product_version
            elif global_attr.lower() == PRODUCT_VERSION:
                attr = nc.getncattr(global_attr)
                drs[PRODUCT_VERSION] = attr
                tags[PRODUCT_VERSION] = attr

        if self.__verbose:
            print("VARIABLES...")
        for (var_id, var) in nc.variables.items():
            if self.__verbose:
                print("\tVARIABLE ATTRIBUTES (%s)" % var_id)
            for attr in var.ncattrs():
                if self.__verbose:
                    print("\t%s=%s" % (attr, var.getncattr(attr)))
                if (attr.lower() == 'long_name' and
                        len(var.getncattr(attr)) == 0):
                    self.__error_messages.add(
                        'WARNING in %s, long_name value has zero length' % ds)

        return drs, tags

    def _process_file_atrib(self, global_attr, attr, ds):
        drs = {}
        tags = {}
        if self.__use_mapping:
            if global_attr != INSTITUTION and '(' in attr:
                attr = attr.split(')')
                tmp_attr = ''
                first = True
                for bit in attr:
                    if bit == '':
                        continue
                    if first:
                        first = False
                        tmp_attr = '%s%s' % (tmp_attr, bit.split('(', 1)[1])
                    else:
                        tmp_attr = '%s,%s' % (tmp_attr, bit.split('(', 1)[1])
                attr = tmp_attr
            attr = attr.replace('merged: ', '')

        if '<' in attr:
            bits = attr.split(', ')
        else:
            bits = attr.split(',')

        # Hack to deal with different variations
        if global_attr == PLATFORM:
            if 'NOAA-<12,14,15,16,17,18>' in bits:
                bits.remove('NOAA-<12,14,15,16,17,18>')
                bits.extend(
                    ['NOAA-12', 'NOAA-14', 'NOAA-15', 'NOAA-16', 'NOAA-17',
                     'NOAA-18'])
            if 'ERS-<1,2>' in bits:
                bits.remove('ERS-<1,2>')
                bits.extend(['ERS-1', 'ERS-2'])
        if self.__use_mapping:
            if global_attr == SENSOR:
                if 'MERISAATSR' in bits:
                    bits.remove('MERISAATSR')
                    bits.extend(['MERIS', 'AATSR'])
                if ' OMI and GOME-2.' in bits:
                    bits.remove(' OMI and GOME-2.')
                    bits.extend(['OMI', 'GOME-2'])
            if global_attr == INSTITUTION:
                if ('University of Leicester (UoL), UK' in attr or
                        'University of Leicester, UK' in attr):
                    bits.remove(' UK')

        term_count = 0
        for bit in bits:
            term_uri = self._get_term_uri(global_attr, bit.strip())
            if term_uri is not None:
                drs[global_attr] = (TripleStore.get_pref_label(term_uri))
                if term_count == 0:
                    tags[global_attr] = set()
                tags[global_attr].add(term_uri)
                term_count = term_count + 1

                if global_attr == PLATFORM:
                    # add the broader terms
                    for tag in self._get_programme_group(term_uri):
                        tags[global_attr].add(tag)

            elif global_attr == PLATFORM:
                # This is an unknown platform
                p_tags = self._get_paltform_as_programme(bit.strip())
                if len(p_tags) > 0 and term_count == 0:
                    tags[PLATFORM] = set()
                    # we are adding a programme or group to the list of
                    # platforms, hence adding more than one platform to the
                    # count to ensure encoded as multi platform
                    term_count = term_count + 2
                for tag in p_tags:
                    tags[PLATFORM].add(tag)

        if term_count > 1 and global_attr == SENSOR:
            drs[global_attr] = self.MULTI_SENSOR
        elif term_count > 1 and global_attr == PLATFORM:
            drs[global_attr] = self.MULTI_PLATFORM

        if drs == {}:
            self.__error_messages.add('ERROR in %s for %s, invalid value "%s"'
                                      % (ds, global_attr, attr))

        return drs, tags

    def _get_programme_group(self, term_uri):
        # now add the platform programme and group
        tags = []
        programme = self.__facets.get_platforms_programme(term_uri)
        programme_uri = self._get_term_uri(
            PLATFORM_PROGRAMME, programme)
        tags.append(programme_uri)
        try:
            group = self.__facets.get_programmes_group(programme_uri)
            group_uri = self._get_term_uri(PLATFORM_GROUP, group)
            tags.append(group_uri)
        except KeyError:
            # not all programmes have groups
            pass
        return tags

    def _get_paltform_as_programme(self, platform):
        tags = []
        # check if the platform is really a platform programme
        if (platform in self.__facets.get_programme_labels()):
            programme_uri = self._get_term_uri(PLATFORM_PROGRAMME, platform)
            tags.append(programme_uri)
            try:
                group = self.__facets.get_programmes_group(programme_uri)
                group_uri = self._get_term_uri(PLATFORM_GROUP, group)
                tags.append(group_uri)
            except KeyError:
                # not all programmes have groups
                pass

        # check if the platform is really a platform group
        elif (platform in self.__facets.get_group_labels()):
            group_uri = self._get_term_uri(PLATFORM_GROUP, platform)
            tags.append(group_uri)

        return tags

    def _generate_ds_id(self, ds, drs_ds):
        error = False
        facets = [ECV, FREQUENCY, PROCESSING_LEVEL, DATA_TYPE, SENSOR,
                  PLATFORM, PRODUCT_STRING, PRODUCT_VERSION]
        ds_id = self.DRS_ESACCI
        for facet in facets:
            try:
                if drs_ds[facet] == '':
                    error = True
                    message_found = False
                    # Do not add another message if we have already reported an
                    # invalid value
                    for message in self.__error_messages:
                        if (message.startswith(
                                'ERROR in %s for %s, invalid value' %
                                (ds, facet))):
                            message_found = True
                    if not message_found:
                        self.__error_messages.add(
                            'ERROR in %s for %s, value not found' %
                            (ds, facet))

                else:
                    facet_value = str(drs_ds[facet]).replace(
                        '.', '-').replace(' ', '-')
                    if facet == FREQUENCY:
                        facet_value = facet_value.replace(
                            'month', 'mon').replace('year', 'yr')
                    ds_id = '%s.%s' % (ds_id, facet_value)
            except(KeyError):
                error = True
                message_found = False
                # Do not add another message if we have already reported an
                # invalid value
                for message in self.__error_messages:
                    if (message.startswith('ERROR in %s for %s, invalid value'
                                           % (ds, facet))):
                        message_found = True
                if not message_found:
                    self.__error_messages.add(
                        'ERROR in %s for %s, value not found' % (ds, facet))
        if error:
            return None

        return ds_id

    def _get_next_realization(self, ds, drs_id, drs):
        # first check if there was an existing DRS for this dataset
        existing_drs = self.__drs_mapping.get_drs(ds)
        if existing_drs:
            for e_drs in existing_drs:
                if e_drs.startswith(drs_id):
                    # return the realization from the existing DRS
                    return (e_drs.split(drs_id + '.'))[1]

        # generate a new realization
        realization_no = 1
        while True:
            ds_id_r = '%s.r%s' % (drs_id, realization_no)
            if ((ds_id_r not in self.__drs_mapping.get_drs()) and
                    (ds_id_r not in drs.keys())):
                return 'r%s' % (realization_no)
            realization_no = realization_no + 1

    def _write_csv(self, ds, drs):
        single_values = [DATA_TYPE, ECV, PROCESSING_LEVEL, PRODUCT_STRING]
        multi_values = [FREQUENCY, INSTITUTION, PLATFORM, SENSOR]
        for value in single_values:
            try:
                self.__file_csv.write('%s,%s\n' % (ds, drs[value]))
            except KeyError:
                pass

        for value in multi_values:
            try:
                for uri in drs[value]:
                    self.__file_csv.write('%s,%s\n' % (ds, uri))
            except KeyError:
                pass

    def _write_json(self, drs):
        self.__file_drs.write(
            json.dumps(drs, sort_keys=True, indent=4, separators=(',', ': ')))

    def _get_term_uri(self, facet, term, ds=None):
        facet = facet.lower()
        term_l = self._convert_term(facet, term)
        if term_l in self.__facets.get_labels(facet).keys():
            return self.__facets.get_labels(facet)[term_l]
        elif term_l in self.__facets.get_alt_labels(facet).keys():
            return self.__facets.get_alt_labels(facet)[term_l]
        self.__not_found_messages.add("%s: %s" % (facet, term))
        if ds:
            self.__error_messages.add(
                'ERROR in %s for %s, invalid value "%s"' %
                (ds, facet, term))

    def _convert_term(self, facet, term):
        term = term.lower()
        if self.__use_mapping:
            for key in LocalFacetMappings.get_mapping(facet).keys():
                if term == key.lower():
                    return LocalFacetMappings.get_mapping(facet)[key].lower()
        return term

    def _open_files(self, ):
        self.__file_csv = open('tags.csv', 'w')
        self.__file_drs = open('drs.json', 'w')
        self.__file_error = open('error.txt', 'w')

    def _close_files(self, ):
        self.__file_csv.close()
        self.__file_drs.close()
        self.__file_error.close()
