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

import csv

from cci_tagger.constants import FREQUENCY, INSTITUTION, PLATFORM, SENSOR,\
    PROCESSING_LEVEL
from cci_tagger.settings import DS_DRS_FILE


class LocalFacetMappings(object):
    """
    These mappings are used to map from values found in the files to the terms
    used in the vocab server.

    """

    __freq = {
        'daily': 'day',
    }

    __institute = {
        'DTU Space - Div. of Geodynamics': 'DTU Space',
        'DTU Space - Div. of Geodynamics and NERSC': 'DTU Space',
        'DTU Space - Microwaves and Remote Sensing': 'DTU Space',
        'Deutsches Zentrum fuer Luft- und Raumfahrt (DLR)':
        'Deutsches Zentrum fuer Luft- und Raumfahrt',
        'ESACCI': 'ESACCI_SST',
        'Plymouth Marine Laboratory Remote Sensing Group':
        'Plymouth Marine Laboratory',
        'Royal Netherlands Meteorological Institute (KNMI)':
        'Royal Netherlands Meteorological Institute',
        'SRON Netherlands Institute for Space Research':
        'Netherlands Institute for Space Research',
        'University of Leicester (UoL)': 'University of Leicester',
    }

    __level = {
        'level-3': 'l3',
    }

    __platform = {
        'ERS2': 'ERS-2',
        'ENV': 'ENVISAT',
        'EOS-AURA': 'AURA',
        'MetOpA': 'Metop-A',
        'Nimbus 7': 'Nimbus-7',
        'orbview-2/seastar': 'orbview-2',
        'SCISAT': 'SCISAT-1',
    }

    __sensor = {
        'AMSR-E': 'AMSRE',
        'ATSR2': 'ATSR-2',
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

    __mappings = {}
    __mappings[FREQUENCY] = __freq
    __mappings[INSTITUTION] = __institute
    __mappings[PROCESSING_LEVEL] = __level
    __mappings[PLATFORM] = __platform
    __mappings[SENSOR] = __sensor

    @classmethod
    def __str__(cls):
        """
        Get the string representation.

        @return the str representation of this class

        """
        output = ''
        for scheme in cls.__mappings.keys():
            scheme_dict = cls.get_mapping(scheme)
            if len(scheme_dict) > 0:
                output = ('%s\nMappings for %s:\n' % (output, scheme))
                for key in scheme_dict.keys():
                    output = ('%s\tfrom\t %s\n\tto\t %s\n' %
                              (output, key, scheme_dict[key]))
        return output

    @classmethod
    def get_mapping(cls, facet):
        """
        Get the mappings for the given facet.

        @param facet (str): the name of the facet

        @return a dict where:\n
                key = attrib name\n
                value = vocab label\n
                The dict may be empty. An empty dict is returned for unknown
                facet.

        """
        if facet in cls.__mappings.keys():
            return cls.__mappings[facet]
        return {}

    @classmethod
    def get_facet(cls):
        """
        Get the list of facets that mappings are available for.

        @return a list(str) the names of the known facets

        """
        return cls.__mappings.keys()


class DRS_Mapping(object):
    """
    This class provides information about datasets and associated DRS.

    """
    # a dict, key: dataset(internal path), value: set of DRS
    __ds_drs = {}

    # a list of the DRS
    __drs = []

    def __init__(self):
        """
        Initialise the class.

        """
        try:
            with open(DS_DRS_FILE, 'rb') as csvfile:
                cvsreader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in cvsreader:
                    if row[0] in self.__ds_drs.keys():
                        self.__ds_drs[row[0]].add(row[1])
                    else:
                        self.__ds_drs[row[0]] = set([row[1]])
                    self.__drs.append(row[1])
        except IOError:
            print('WARNING file {file} not found. Unable to initialise '
                  'dataset DRS mappings'.format(file=DS_DRS_FILE))

    def get_drs(self, ds=None):
        """
        Get the list of the DRS for the given dataset.

        @param ds (str): the name of the dataset, may be None

        @return a list(str) of the DRS for the given dataset. If the value of
                ds is None a list of all of the DRS is returned.
        """
        if ds is None:
            return self.__drs
        return self.__ds_drs.get(ds)

    def output_drs(self, ds_drs):
        """
        Merge the inputed values with the stored values and then write the
        resulting data to a file.

        @param ds_drs (set): the name of the dataset and the DRS, as comma
                separated values

        """
        for key in self.__ds_drs.keys():
            for drs in self.__ds_drs[key]:
                ds_drs.add('{key},{drs}'.format(key=key, drs=drs))
        file_ds_drs = open(DS_DRS_FILE, 'w')
        for message in sorted(ds_drs):
            file_ds_drs.write('%s\n' % message)
        file_ds_drs.close()
