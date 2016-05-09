'''
BSD Licence
Copyright (c) 2016, Science & Technology Facilities Council (STFC)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

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

import json
import os
import time

from rdflib import ConjunctiveGraph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

import netCDF4


DEPTH = 1000

VERBOSE = True
VERBOSE = False

USE_MAPPING = False
USE_MAPPING = True

SCHEMES = {}
NOT_FOUND = set()
NOT_FOUND_FULL = set()

FILE_DRS = None
FILE_CSV = None
FILE_ERROR = None


FREQ_MAPPING = {
    'daily': 'day',
}

INSTITUTION_MAPPING = {
    'DTU Space - Div. of Geodynamics and NERSC': 'DTU Space',
    'DTU Space - Microwaves and Remote Sensing': 'DTU Space',
    'Deutsches Zentrum fuer Luft- und Raumfahrt (DLR)': 'Deutsches Zentrum fuer Luft- und Raumfahrt',
    'ESACCI': 'ESACCI_SST',
    'Plymouth Marine Laboratory Remote Sensing Group': 'Plymouth Marine Laboratory',
    'Royal Netherlands Meteorological Institute (KNMI)': 'Royal Netherlands Meteorological Institute',
    'SRON Netherlands Institute for Space Research': 'Netherlands Institute for Space Research',
    'University of Leicester (UoL)': 'University of Leicester',
}

LEVEL_MAPPING = {
    'level-3': 'l3',
}

PLATFORM_MAPPING = {
    'ENV': 'ENVISAT',
    'EOS-AURA': 'AURA',
    'METOP-A': 'MetOpA',
    'Nimbus 7': 'Nimbus-7',
    'orbview-2/seastar': 'orbview-2',
}

SENSOR_MAPPING = {
    'AMSR-E': 'AMSRE',
    'ATSR-2': 'ATSR',
    'AVHRR GAC': 'AVHRR',
    'AVHRR_GAC': 'AVHRR',
    'AVHRR_HRPT': 'AVHRR',
    'AVHRR_LAC': 'AVHRR',
    'AVHRR_MERGED': 'AVHRR',
    'GFO': 'GFO-RA',
    'MERIS_FRS': 'MERIS',
    'MERIS_RR': 'MERIS',
    'MODIS_MERGED': 'MODIS',
    'RA2': 'RA-2',
    'SMR_544.6GHz': 'SMR',
}

CONVERSIONS = {}
CONVERSIONS['data_type'] = {}
CONVERSIONS['ecv'] = {}
CONVERSIONS['product'] = {}
CONVERSIONS['time_coverage_resolution'] = FREQ_MAPPING
CONVERSIONS['institution'] = INSTITUTION_MAPPING
CONVERSIONS['processing_level'] = LEVEL_MAPPING
CONVERSIONS['platform'] = PLATFORM_MAPPING
CONVERSIONS['sensor'] = SENSOR_MAPPING


## SPARQL STUFF ##

SPARQL_HOST_NAME = 'vocab-test.ceda.ac.uk'
SPARQL_DATASET = 'vocab'
SPARQL_QUERY = 'http://%s/%s/sparql' % (SPARQL_HOST_NAME, SPARQL_DATASET)

MULTI_SENSOR = 'multi-sensor'
MULTI_PLATFORM = 'multi-platform'

# This allows us to use the prefix values in the queries rather than the url
PREFIX = """
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>
"""

GRAPH = None


def get_graph():
    global GRAPH
    if GRAPH == None:
        store = SPARQLStore(endpoint=SPARQL_QUERY)
        GRAPH = ConjunctiveGraph(store=store)
    return GRAPH


def get_concepts_in_scheme(uri):
    graph = get_graph()
    statement = PREFIX + \
        'SELECT ?concept ?label WHERE { GRAPH ?g {?concept skos:inScheme <' + uri + \
        '> . ?concept skos:prefLabel ?label} }'
    result_set = graph.query(statement)
    concepts = {}
    for result in result_set:
        concepts[("" + result.label).lower()] = result.concept.decode()
    return concepts


def get_alt_concepts_in_scheme(uri):
    graph = get_graph()
    statement = PREFIX + \
        'SELECT ?concept ?label WHERE { GRAPH ?g {?concept skos:inScheme <' + \
        uri + '> . ?concept skos:altLabel ?label} }'
    result_set = graph.query(statement)
    concepts = {}
    for result in result_set:
        concepts[("" + result.label).lower()] = result.concept.decode()
    return concepts


def get_pref_label(uri):
    # get the pref label for a uri
    graph = get_graph()
    statement = PREFIX + \
        'SELECT ?label WHERE { GRAPH ?g {<%s> skos:prefLabel ?label} }' % uri
    results = graph.query(statement)
    # there should only be one result
    for resource in results:
        return resource.label.decode()
    return ''


def get_alt_label(uri):
    # get the alt label for a uri
    graph = get_graph()
    statement = PREFIX + \
        'SELECT ?label WHERE { GRAPH ?g {<%s> skos:altLabel ?label} }' % uri
    results = graph.query(statement)
    # there should only be one result
    for resource in results:
        return resource.label.decode()
    return ''

## END OF SPARQL STUFF ##

## PROCESS FILE NAMES ##


def get_nc_files(_dir):
    file_list = []
    count = 1
    for root, _, files in os.walk(_dir):
        for name in files:
            if name.endswith('.nc'):
                file_list.append(os.path.join(root, name))
                count = count + 1
                if count > DEPTH:
                    return file_list
    return file_list


def scan_net_cdf_file(fpath, ds):
    """
    Extract data from the net cdf file.

    The values to extract are take from the know_attr list which are the keys of
    the attr_mapping dictionary.
    """

    drs = {}
    tags = {}
    try:
        nc = netCDF4.Dataset(fpath)
    except:
        FILE_ERROR.write("ERROR processing file %s\n" % fpath)
        return drs, tags

    if VERBOSE:
        print "GLOBAL ATTRS for %s: " % fpath
    for global_attr in nc.ncattrs():
        if VERBOSE:
            print global_attr, "=", nc.getncattr(global_attr)

        if global_attr.lower() in know_attr:
            attr = nc.getncattr(global_attr)
            if USE_MAPPING:
                if global_attr != 'institution' and '(' in attr:
                    attr = attr.split(')')
                    tmp_attr = ''
                    first = True
                    for bit in attr:
                        if bit == '':
                            continue
                        if first:
                            first = False
                            tmp_attr = '%s%s' % (
                                tmp_attr, bit.split('(', 1)[1])
                        else:
                            tmp_attr = '%s,%s' % (
                                tmp_attr, bit.split('(', 1)[1])
                    attr = tmp_attr
                attr = attr.replace('merged: ', '')

            if '<' in attr:
                bits = attr.split(', ')
            else:
                bits = attr.split(',')

            # Hack to deal with different variations
            if global_attr == 'platform':
                if 'NOAA-<12,14,15,16,17,18>' in bits:
                    bits.remove('NOAA-<12,14,15,16,17,18>')
                    bits.extend(
                        ['NOAA-12', 'NOAA-14', 'NOAA-15', 'NOAA-16', 'NOAA-17', 'NOAA-18'])
                if 'ERS-<1,2>' in bits:
                    bits.remove('ERS-<1,2>')
                    bits.extend(['ERS-1', 'ERS-2'])
            if USE_MAPPING:
                if global_attr == 'sensor':
                    if 'MERISAATSR' in bits:
                        bits.remove('MERISAATSR')
                        bits.extend(['MERIS', 'AATSR'])
                    if ' OMI and GOME-2.' in bits:
                        bits.remove(' OMI and GOME-2.')
                        bits.extend(['OMI', 'GOME-2'])
                if global_attr == 'institution':
                    if ('University of Leicester (UoL), UK' in attr
                            or 'University of Leicester, UK' in attr):
                        bits.remove(' UK')

            term_count = 0
            none_found = False
            for bit in bits:
                term = get_term_uri(global_attr, bit.strip())
                if term != None:
                    drs[attr_mapping[global_attr.lower()]] = get_pref_label(
                        term)
                    if term_count == 0:
                        tags[attr_mapping[global_attr.lower()]] = set()
                    tags[attr_mapping[global_attr.lower()]].add(term)
                    term_count = term_count + 1
                else:
                    none_found = True
            if term_count > 1 and global_attr == 'sensor':
                drs[attr_mapping[global_attr.lower()]] = MULTI_SENSOR
            elif term_count > 1 and global_attr == 'platform':
                drs[attr_mapping[global_attr.lower()]] = MULTI_PLATFORM

            if none_found:
                NOT_FOUND_FULL.add(
                    "%s: %s - %s" % (global_attr, nc.getncattr(global_attr), ds))

        # we don't have a vocab for product_version
        elif global_attr.lower() == 'product_version':
            drs['product_version'] = nc.getncattr(global_attr)
            tags['product_version'] = nc.getncattr(global_attr)

    if VERBOSE:
        print "VARIABLES..."
        for (var_id, var) in nc.variables.items():
            print var_id, var
            print "\tVARIABLE ATTRIBUTES (%s)" % var_id
        for attr in var.ncattrs():
            print "\t", attr, "=", var.getncattr(attr)
    return drs, tags


def parse_file_name(ds, fpath):
    """
    Extract data from the file name. 

    The file name comes in two different formats. The values are '-' delimited. 
    Form 1
        <Indicative Date>[<Indicative Time>]-ESACCI
        -<Processing Level>_<CCI Project>-<Data Type>-<Product String>
        [-<Additional Segregator>][-v<GDS version>]-fv<File version>.nc
    Form 2
        ESACCI-<CCI Project>-<Processing Level>-<Data Type>-<Product String>
        [-<Additional Segregator>]-<IndicativeDate>[<Indicative Time>]
        -fv<File version>.nc

    Values extracted from the file name:
        Processing Level - level
        CCI Project - ecv_id
        Data Type - variable
        Product String - product_id
    """

    form1 = {}
    form2 = {}
    path_facet_bits = fpath.split('/')
    last_bit = len(path_facet_bits) - 1
    file_segments = path_facet_bits[last_bit].split('-')
    count = 0
    if len(file_segments) < 2:
        FILE_ERROR.write(
            'ERROR with format of file name %s from %s\n' % (path_facet_bits[last_bit], ds))
        return {}, {}
    for segment in file_segments:
        count = count + 1
        if file_segments[1] == 'ESACCI':
            if count == 7 and segment.startswith('fv'):
                count = 8
            form1[count] = segment
        elif file_segments[0] == 'ESACCI':
            form2[count] = segment
        else:
            if count == 1:
                bits = segment.split('_')
                if len(bits) > 1:
                    form1[count] = bits[0]
                    count = count + 1
                    form1[count] = bits[1]
                else:
                    form1[count] = segment
            elif count == 2 and segment.endswith('.nc'):
                form1[9] = segment
            elif count > 6:
                if segment.startswith('v'):
                    form1[7] = segment
                elif segment.startswith('fv'):
                    form1[8] = segment
                else:
                    form1[count] = segment
            else:
                form1[count] = segment

    if form1:
        return process_form_1(ds, form1)
    if form2:
        return process_form_2(ds, form2)


def process_form_1(ds, form):
    csv_rec = {}
    try:
        term = get_term_uri('processing_level', form[3].split('_')[0], ds)
        if term != None:
            csv_rec['level'] = term
        term = get_term_uri('ecv', form[3].split('_')[1], ds)
        if term != None:
            csv_rec['ecv_id'] = term
    except(KeyError):
        pass
    try:
        term = get_term_uri('data_type', form[4], ds)
        if term != None:
            csv_rec['variable'] = term
    except(KeyError):
        pass
    try:
        term = get_term_uri('product', form[5], ds)
        if term != None:
            csv_rec['product_id'] = term
    except(KeyError):
        pass
    return create_drs_record(csv_rec), csv_rec


def process_form_2(ds, form):
    csv_rec = {}
    try:
        term = get_term_uri('processing_level', form[3], ds)
        if term != None:
            csv_rec['level'] = term
    except(KeyError):
        pass
    try:
        term = get_term_uri('ecv', form[2], ds)
        if term != None:
            csv_rec['ecv_id'] = term
    except(KeyError):
        pass
    try:
        term = get_term_uri('data_type', form[4], ds)
        if term != None:
            csv_rec['variable'] = term
    except(KeyError):
        pass
    try:
        term = get_term_uri('product', form[5], ds)
        if term != None:
            csv_rec['product_id'] = term
    except(KeyError):
        pass
    return create_drs_record(csv_rec), csv_rec


def create_drs_record(csv_rec):
    proc_lev_label = get_alt_label(csv_rec.get('level'))
    project_label = get_alt_label(csv_rec.get('ecv_id'))
    data_type_label = get_alt_label(csv_rec.get('variable'))
    product_label = get_pref_label(csv_rec.get('product_id'))
    drs = {'project': 'esacci', 'ensemble': 'r1'}
    if project_label != '':
        drs['ecv_id'] = project_label
    if proc_lev_label != '':
        drs['level'] = proc_lev_label
    if data_type_label != '':
        drs['variable'] = data_type_label
    if product_label != '':
        drs['product_id'] = product_label
    return drs


def process_datasets(datasets):
    # loop through the datasets pulling out data from file names and from
    # within net cdf files
    ds_len = len(datasets)
    print "Processing %s datasets" % ds_len
    drs = {}
    count = 0
    for ds in datasets:
        count = count + 1
        tags_ds = {}
        # get a list of files
        nc_files = get_nc_files(ds)
        print "Dataset %s of %s Processing %s files from %s" % (count, ds_len, len(nc_files), ds)
        for fpath in nc_files:
            drs_ds = {}

            net_cdf_drs, net_cdf_tags = parse_file_name(ds, fpath)
            drs_ds.update(net_cdf_drs)
            tags_ds.update(net_cdf_tags)
            net_cdf_drs, net_cdf_tags = scan_net_cdf_file(fpath, ds)
            drs_ds.update(net_cdf_drs)
            tags_ds.update(net_cdf_tags)

#         if drs_ds == {}:
#             FILE_ERROR.write('%s,no data extracted\n' % ds)
            dataset_id = generate_ds_id(ds, drs_ds)

            if dataset_id:
                # only add files with all of the drs data
                if dataset_id in drs.keys():
                    #                     FILE_ERROR.write("ERROR duplicate drs id %s\n" % dataset_id)
                    drs[dataset_id].append(fpath)
                else:
                    drs[dataset_id] = [fpath]

        write_csv(ds, tags_ds)

    write_json(drs)

    if len(NOT_FOUND) > 0:
        print "\nNOT IN VOCAB, SUMMARY:\n"
    for nf in sorted(NOT_FOUND):
        print nf
    if len(NOT_FOUND_FULL) > 0:
        print "\nNOT IN VOCAB, DETAIL:\n"
    for nf in sorted(NOT_FOUND_FULL):
        print nf


def generate_ds_id(ds, drs):
    error = False
    facets = ['ecv_id', 'freq', 'level', 'variable', 'sensor_id',
              'platform_id', 'product_id', 'product_version', 'ensemble']
    ds_id = 'esacci'
    for facet in facets:
        try:
            if drs[facet] == '':
                error = True
                FILE_ERROR.write('%s,%s\n' % (ds, facet))
            else:
                facet_value = str(drs[facet]).replace(
                    '.', '-').replace(' ', '-')
                if facet == 'freq':
                    facet_value = facet_value.replace(
                        'month', 'mon').replace('year', 'yr')
                ds_id = '%s.%s' % (ds_id, facet_value)
        except(KeyError):
            error = True
            FILE_ERROR.write('%s,%s\n' % (ds, facet))
    if error:
        return None
    return ds_id


def write_csv(ds, drs):
    single_values = ['variable', 'ecv_id', 'level', 'product_id']
    multi_values = ['freq', 'institution', 'platform_id', 'sensor_id']
    for value in single_values:
        try:
            FILE_CSV.write('%s,%s\n' % (ds, drs[value]))
        except KeyError:
            pass

    for value in multi_values:
        try:
            for uri in drs[value]:
                FILE_CSV.write('%s,%s\n' % (ds, uri))
        except KeyError:
            pass


def write_json(drs):
    FILE_DRS.write(
        json.dumps(drs, sort_keys=True, indent=4, separators=(',', ': ')))


def get_term_uri(scheme, term, ds=None):
    scheme = scheme.lower()
    term_l = convert_term(scheme, term)
    if term_l in SCHEMES[scheme].keys():
        return SCHEMES[scheme][term_l]
    elif term_l in SCHEMES[scheme + '-alt'].keys():
        return SCHEMES[scheme + '-alt'][term_l]
    NOT_FOUND.add("%s: %s" % (scheme, term))
    if ds:
        NOT_FOUND_FULL.add("%s: %s - %s" % (scheme, term, ds))


def convert_term(scheme, term):
    term = term.lower()
    if USE_MAPPING:
        for key in CONVERSIONS[scheme].keys():
            if term == key.lower():
                return CONVERSIONS[scheme][key].lower()
    return term


def open_files():
    global FILE_CSV, FILE_DRS, FILE_ERROR
    FILE_CSV = open('tags.csv', 'w')
    FILE_DRS = open('drs.json', 'w')
    FILE_ERROR = open('error.txt', 'w')


def close_files():
    FILE_CSV.close()
    FILE_DRS.close()
    FILE_ERROR.close()

if __name__ == "__main__":
    print "\n\n\n\n\n%s STARTED" % (time.strftime("%H:%M:%S"))

    attr_mapping = {'time_coverage_resolution': 'freq', 'institution': 'institution',
                    'platform': 'platform_id', 'sensor': 'sensor_id'}
    know_attr = attr_mapping.keys()
    SCHEMES['data_type'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/dataType')
    SCHEMES['data_type-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/dataType')
    SCHEMES['ecv'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/ecv')
    SCHEMES['ecv-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/ecv')
    SCHEMES['time_coverage_resolution'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/freq')
    SCHEMES['time_coverage_resolution-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/freq')
    SCHEMES['platform'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/platform')
    SCHEMES['platform-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/platform')
    SCHEMES['processing_level'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/procLev')
    SCHEMES['processing_level-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/procLev')
    SCHEMES['sensor'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/sensor')
    SCHEMES['sensor-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/sensor')
    SCHEMES['institution'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/org')
    SCHEMES['institution-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/org')
    SCHEMES['product'] = get_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/product')
    SCHEMES['product-alt'] = get_alt_concepts_in_scheme(
        'http://vocab-test.ceda.ac.uk/scheme/cci/product')

    aerosol = {
        '/neodc/esacci_aerosol/data/MS_UVAI/L3_MONTHLY/v1.4.7',
        '/neodc/esacci_aerosol/data/MS_UVAI/L3_DAILY/v1.4.7',
        '/neodc/esacci_aerosol/data/MS_UVAI/L3_CLIMATOLOGY/v1.4.7',
        '/neodc/esacci_aerosol/data/MS_UVAI/IMAGES_MONTHLY/v1.4.7',
        '/neodc/esacci_aerosol/data/MS_UVAI/IMAGES_CLIMATOLOGY/v1.4.7',
        '/neodc/esacci_aerosol/data/GOMOS_AERGOM/L3_MONTHLY/v2.1',
        '/neodc/esacci_aerosol/data/ATSR2_SU//L3_MONTHLY//v4.2',  # duff ?
        '/neodc/esacci_aerosol/data/ATSR2_SU/L3_DAILY/v4.2',
        '/neodc/esacci_aerosol/data/ATSR2_SU/L2/v4.2',
        '/neodc/esacci_aerosol/data/AATSR_SU/L3_MONTHLY/v4.2',
        '/neodc/esacci_aerosol/data/AATSR_SU/L3_DAILY/v4.2',
        '/neodc/esacci_aerosol/data/AATSR_SU/L2/v4.2',
    }

    cloud = {
        '/neodc/esacci_cloud/data/L3U/aatsr_envisat',
        '/neodc/esacci_cloud/data/L3U/avhrr_noaa-15',
        '/neodc/esacci_cloud/data/L3U/avhrr_noaa-16',
        '/neodc/esacci_cloud/data/L3U/avhrr_noaa-17',
        '/neodc/esacci_cloud/data/L3U/avhrr_noaa-18',
        '/neodc/esacci_cloud/data/L3U/merisaatsr_envisat',
        '/neodc/esacci_cloud/data/L3U/modis_aqua',
        '/neodc/esacci_cloud/data/L3U/modis_terra',
        '/neodc/esacci_cloud/data/L3C/modis_terra',
        '/neodc/esacci_cloud/data/L3S/modis_merged',
        '/neodc/esacci_cloud/data/L3C/modis_aqua',
        '/neodc/esacci_cloud/data/L3C/merisaatsr_envisat',
        '/neodc/esacci_cloud/data/L3S/merged',
        '/neodc/esacci_cloud/data/L3C/avhrr_noaa-18',
        '/neodc/esacci_cloud/data/L3C/avhrr_noaa-17',
        '/neodc/esacci_cloud/data/L3C/avhrr_noaa-16',
        '/neodc/esacci_cloud/data/L3C/avhrr_noaa-15',
        '/neodc/esacci_cloud/data/L3S/avhrr_merged/',  # duff ?
        '/neodc/esacci_cloud/data/L3C/aatsr_envisat',
    }

    fire = {
        '/neodc/esacci_fire/data/burned_area/pixel/v3.1',
        '/neodc/esacci_fire/data/burned_area/grid',
        '/neodc/esacci_fire/data/burned_area/pixel',
    }

    glaciers = {
        '/neodc/esacci_glaciers/data/randolph_glacier_inventory/gridded/v5.0',
    }

    ghg = {
        '/neodc/esacci_ghg/data/crdp_2/L2/SCIAMACHY/CO2_SCI_BESD/v02.00.08',
        '/neodc/esacci_ghg/data/crdp_2/L2/SCIAMACHY/CO2_SCI_WFMD/v3.8',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CO2_GOS_BESD/v02.00.08',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CO2_GOS_SRFP/v2.3.6',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CO2_GOS_OCFP/v5.2',
        '/neodc/esacci_ghg/data/crdp_2/L2/merged/CO2_EMMA/v2.0',
        '/neodc/esacci_ghg/data/crdp_2/L2/SCIAMACHY/CH4_SCI_WFMD/v3.7',
        '/neodc/esacci_ghg/data/crdp_2/L2/SCIAMACHY/CH4_SCI_IMAP/v7.0',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CH4_GOS_SRPR/v2.3.6',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CH4_GOS_OCPR/v5.2',
        '/neodc/esacci_ghg/data/crdp_2/L2/GOSAT/CH4_GOS_SRFP/v2.3.6',
    }

    ice_sheet = {
        '/neodc/esacci/ice_sheets_greenland/',
        '/neodc/esacci/ice_sheets_greenland/',
        '/neodc/esacci/ice_sheets_greenland/',
        '/neodc/esacci/ice_sheets_greenland/',
        '/neodc/esacci/greenland_ice_sheets',
        '/neodc/esacci/greenland_ice_sheets',
        '/neodc/esacci/greenland_ice_sheets',
        '/neodc/esacci/greenland_ice_sheets',
        '/neodc/esacci/ice_sheets_greenland/',
    }

    land_cover = {
        '/neodc/esacci/land_cover/data/land_cover_maps/v1.6.1',
    }

    ocean_colour = {
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/all_products/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/all_products/8day/v1.0',
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/all_products/monthly/v1.0',
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/chlor_a/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/kd/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/sinusoidal/netcdf/rrs/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/rrs/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/kd/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/iop/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/chlor_a/daily/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/all_products/annual/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/all_products/monthly/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/all_products/8day/v1.0',
        '/neodc/esacci_oc/data/v1-release/geographic/netcdf/all_products/daily/v1.0',
    }

    ozone = {
        '/neodc/esacci/ozone/data/total_columns/l3/merged/v0100',
        '/neodc/esacci/ozone/data/nadir_profiles/l3/merged/v0002',
        '/neodc/esacci/ozone/data/limb_profiles/l3/merged/merged_semi_monthly_mean/v0002',
        '/neodc/esacci/ozone/data/limb_profiles/l3/merged/merged_monthly_zonal_mean/v0002',
        '/neodc/esacci/ozone/data/limb_profiles/l3/sciamachy_envisat/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/osiris/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/smr_odin/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/smr_odin_544_6/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/mipas_envisat/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/gomos_envisat/monthly_zonal_mean/v0001',
        '/neodc/esacci/ozone/data/limb_profiles/l3/ace_fts_scisat/monthly_zonal_mean/v0001',
    }

    sea_ice = {
        '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi/v1.11/SouthernHemisphere',
        '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/amsr-e/v1.11/SouthernHemisphere',
        '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/amsr-e/v1.11/NorthernHemisphere',
        '/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi/v1.11/NorthernHemisphere',
    }

    sea_level = {
        '/neodc/esacci_sealevel/data/IND/v1.1',
        '/neodc/esacci_sealevel/data/L4/MSLA/v1.1',
    }

    sst = {
        '/neodc/esacci/sst/data/lt/Analysis/L4/v01.1',
        '/neodc/esacci_sst/data/gmpe',
        '/neodc/esacci_sst/data/lt/ATSR/L3U/v01.1',
        '/neodc/esacci_sst/data/lt/ATSR/L3U/v01.0',
        '/neodc/esacci_sst/data/lt/AVHRR',
        '/neodc/esacci_sst/data/lt/Analysis',
    }

    soil_moisture = {
        '/neodc/esacci/soil_moisture/data/daily_files/ACTIVE/v02.2',
        '/neodc/esacci/soil_moisture/data/daily_files/PASSIVE/v02.2',
        '/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v02.1',
        '/neodc/esacci/soil_moisture/data/ancillary/v02.2',
        '/neodc/esacci_soilmoisture/data/ancillary/v02.1',
        '/neodc/esacci_soilmoisture/data/daily_files/COMBINED/v02.1',
        '/neodc/esacci_soilmoisture/data/daily_files/PASSIVE/v02.1',
        '/neodc/esacci_soilmoisture/data/daily_files/ACTIVE/v02.1',
    }

    datasets = {}
    datasets['aerosol'] = aerosol
    datasets['cloud'] = cloud
    datasets['fire'] = fire
    datasets['glaciers'] = glaciers
    datasets['ghg'] = ghg
    datasets['ice_sheet'] = ice_sheet
    datasets['land_cover'] = land_cover
    datasets['ocean_colour'] = ocean_colour
    datasets['ozone'] = ozone
    datasets['sea_ice'] = sea_ice
    datasets['sea_level'] = sea_level
    datasets['sst'] = sst
    datasets['soil_moisture'] = soil_moisture

    FORM1 = {}
    FORM1[1] = 'Indicative Date'
    FORM1[2] = 'ESACCI'
    FORM1[3] = 'Processing Level_CCI Project'
    FORM1[4] = 'Data Type'
    FORM1[5] = 'Product String'
    FORM1[6] = 'Additional Segregator'
    FORM1[7] = 'v<GDS version>'
    FORM1[8] = 'fv<File version>.nc'
    FORM1[9] = 'OTHER'
    FORM2 = {}
    FORM2[1] = 'ESACCI'
    FORM2[2] = 'CCI Project'
    FORM2[3] = 'Processing Level'
    FORM2[4] = 'Data Type'
    FORM2[5] = 'Product String'
    FORM2[6] = 'Additional Segregator'
    FORM2[7] = 'IndicativeDate'
    FORM2[8] = 'fv<File version>.nc'

    open_files()

    all_datasets = set()
    for value in datasets.values():
        all_datasets.update(value)
    process_datasets(all_datasets)

    close_files()

    print "%s FINISHED\n\n" % (time.strftime("%H:%M:%S"))
