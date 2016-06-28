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

from rdflib import ConjunctiveGraph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from cci_tagger.settings import SPARQL_HOST_NAME


class TripleStore(object):
    """
    This class provides methods to query the triple store.

    """

    # an instance of a ConjunctiveGraph
    __graph = None

    # This allows us to use the prefix values in the queries rather than the
    # url
    __prefix = """
    PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>
    """

    @classmethod
    def _get_graph(cls):
        """
        Get the graph, creating a new one if necessary.

        """
        if cls.__graph is None:
            store = SPARQLStore(
                endpoint='http://%s/sparql' % (SPARQL_HOST_NAME))
            cls.__graph = ConjunctiveGraph(store=store)
        return cls.__graph

    @classmethod
    def get_concepts_in_scheme(cls, uri):
        """
        Get the preferred labels of all of the concepts for the given concept
        scheme.

        @param uri (str): the uri of the concept scheme

        @return a dict where:\n
                key = lower case version of the concepts preferred label\n
                value = uri of the concept

        """
        graph = cls._get_graph()
        statement = ('%s SELECT ?concept ?label WHERE { GRAPH ?g {?concept '
                     'skos:inScheme <%s> . ?concept skos:prefLabel ?label} }' %
                     (cls.__prefix, uri))
        result_set = graph.query(statement)
        concepts = {}
        for result in result_set:
            concepts[("" + result.label).lower()] = result.concept.decode()
        return concepts

    @classmethod
    def get_alt_concepts_in_scheme(cls, uri):
        """
        Get the alternative labels of all of the concepts for the given concept
        scheme.

        @param uri (str): the uri of the concept scheme

        @return a dict where:\n
                key = lower case version of the concepts alternative label\n
                value = uri of the concept

        """
        graph = cls._get_graph()
        statement = ('%s SELECT ?concept ?label WHERE { GRAPH ?g {?concept '
                     'skos:inScheme <%s> . ?concept skos:altLabel ?label} }' %
                     (cls.__prefix, uri))
        result_set = graph.query(statement)
        concepts = {}
        for result in result_set:
            concepts[("" + result.label).lower()] = result.concept.decode()
        return concepts

    @classmethod
    def get_pref_label(cls, uri):
        """
        Get the preferred label for the concept with the given uri.

        @param uri (str): the uri of the concept

        @return a str containing the preferred label

        """
        graph = cls._get_graph()
        statement = ('%s SELECT ?label WHERE { GRAPH ?g {<%s> skos:prefLabel '
                     '?label} }' % (cls.__prefix, uri))
        results = graph.query(statement)
        # there should only be one result
        for resource in results:
            return resource.label.decode()
        return ''

    @classmethod
    def get_alt_label(cls, uri):
        """
        Get the alternative label for the concept with the given uri.

        @param uri (str): the uri of the concept

        @return a str containing the alternative label

        """
        graph = cls._get_graph()
        statement = ('%s SELECT ?label WHERE { GRAPH ?g {<%s> skos:altLabel '
                     '?label} }' % (cls.__prefix, uri))
        results = graph.query(statement)
        # there should only be one result
        for resource in results:
            return resource.label.decode()
        return ''

    @classmethod
    def get_broader(cls, uri):
        """
        Get the broader concept for the concept with the given uri.

        @param uri (str): the uri of the concept

        @return a tuple where:\n
                [0] = lower case version of the concepts preferred label\n
                [1] = uri of the concept

        """
        graph = cls._get_graph()
        statement = ('%s SELECT ?concept ?label WHERE { GRAPH ?g {?concept '
                     'skos:narrower <%s> . ?concept skos:prefLabel ?label} }' %
                     (cls.__prefix, uri))
        results = graph.query(statement)
        # there should only be one result
        for resource in results:
            return (resource.label.decode(), resource.concept.decode())
        return ('', '')
