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

from cci_tagger.constants import DATA_TYPE, FREQUENCY, INSTITUTION, PLATFORM,\
    SENSOR, ECV, PLATFORM_PROGRAMME, PLATFORM_GROUP, PROCESSING_LEVEL,\
    PRODUCT_STRING
from cci_tagger.triple_store import TripleStore


class Facets(object):
    """
    This class is used to store information about the facets that is obtained
    from the triple store.

    """
    # a dict of concept schemes
    __facets = None

    # mapping from platform uri to platform programme label
    __platform_programme_mappings = None

    # mapping from platform uri to platform group label
    __programme_group_mappings = None

    def __init__(self):
        """
        Initialise the Facets class.

        """
        if self.__facets is None:
            self._init_facets()
        if self.__platform_programme_mappings is None:
            self._init_platform_mappings()

    def _init_facets(self):
        self.__facets = {}
        self.__facets[DATA_TYPE] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/dataType')
        self.__facets[DATA_TYPE + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/dataType'))
        self.__facets[ECV] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/ecv')
        self.__facets[ECV + '-alt'] = TripleStore.get_alt_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/ecv')
        self.__facets[FREQUENCY] = (
            TripleStore.get_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/freq'))
        self.__facets[FREQUENCY + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/freq'))
        self.__facets[PLATFORM] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/platform')
        self.__facets[PLATFORM + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/platform'))
        self.__facets[PLATFORM_PROGRAMME] = (
            TripleStore.get_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/platformProg'))
        self.__facets[PLATFORM_PROGRAMME + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/platformProg'))
        self.__facets[PLATFORM_GROUP] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/platformGrp')
        self.__facets[PLATFORM_GROUP + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/platformGrp'))
        self.__facets[PROCESSING_LEVEL] = (
            TripleStore.get_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/procLev'))
        self.__facets[PROCESSING_LEVEL + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/procLev'))
        self.__facets[SENSOR] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/sensor')
        self.__facets[SENSOR + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/sensor'))
        self.__facets[INSTITUTION] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/org')
        self.__facets[INSTITUTION + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/org'))
        self.__facets[PRODUCT_STRING] = TripleStore.get_concepts_in_scheme(
            'http://vocab-test.ceda.ac.uk/scheme/cci/product')
        self.__facets[PRODUCT_STRING + '-alt'] = (
            TripleStore.get_alt_concepts_in_scheme(
                'http://vocab-test.ceda.ac.uk/scheme/cci/product'))

    def _init_platform_mappings(self):
        """
        Initialise the platform-programme and programme-group mappings.

        Get the hierarchical mappings between programme, platform and group and
        store the locally.

        """
        self.__platform_programme_mappings = {}
        self.__programme_group_mappings = {}
        for platform in self.__facets[PLATFORM].values():
            program_label, program_uri = TripleStore.get_broader(platform)
            self.__platform_programme_mappings[platform] = program_label
            group_label, _ = TripleStore.get_broader(program_uri)
            if group_label:
                self.__programme_group_mappings[platform] = group_label

    def get_alt_labels(self, facet):
        """
        Get the facet alternative labels and uirs.

        @param facet (str): the name of the facet

        @return a dict where:\n
            key = lower case version of the concepts alternative label\n
            value = uri of the concept

        """
        return self.__facets['{facet}-alt'.format(facet)]

    def get_labels(self, facet):
        """
        Get the facet labels and uirs.

        @param facet (str): the name of the facet

        @return a dict where:\n
            key = lower case version of the concepts preferred label\n
            value = uri of the concept

        """
        return self.__facets[facet]

    def get_platforms_programme(self, uri):
        """"
        Get the programme label for the given platform URI.

        @param uri (str): the URI of the platform

        @return a str containing the label of the programme that contains the
                platform

        """
        return self.__platform_programme_mappings[uri]

    def get_programme_labels(self):
        """
        Get a list of the programme labels, where a programme is a container
        for platforms.

        @return a list of str containing programme labels

        """
        return self.__platform_programme_mappings.values()

    def get_programmes_group(self, uri):
        """"
        Get the group label for the given programme URI.

        @param uri (str): the URI of the programme

        @return a str containing the label of the group that contains the
                programme

        """
        return self.__programme_group_mappings[uri]

    def get_group_labels(self):
        """
        Get a list of the group labels, where a group is a container
        for programmes.

        @return a list of str containing group labels

        """
        return self.__programme_group_mappings.values()
