# -*- coding: utf-8 -*-

"""
connector.py
---
#author	    hjw
#date		2017-07-25
#version	0.0.1
---
<enter your description here>
"""

from elasticsearch import Elasticsearch
from elasticsearch_xpack import XPackClient
import json
from pandas import Series, DataFrame, read_json, concat
import logging
import datetime

logger = logging.getLogger('datageek')


# import numpy as np

class es_server(object):
    def __init__(self, ip='localhost', port='9200'):
        self._server = ip
        self._port = port
        self._client = Elasticsearch([self._server + ':' + self._port])
        self._indices = self._client.indices

    def connect(self, ip='localhost', port='9200'):
        self._server = ip
        self._port = port
        self._client = Elasticsearch([self._server + ':' + self._port])

    def get_version(self):
        version = self._client.info()['version']['number']
        print("*" * 60)
        print("es version is " + version)
        return version

    def load_es_index(self, index, size=None):
        if index is None:
            logger.error("index name is not set.")
            raise Exception('Please specify an exited index in es.')

        if size is None:
            size = 10

        self._client.search(
            index=index,
            size=size,
            body={
                "query": {
                    "match_all": {}
                }
            }
        )

    def load_data_to_pd(self, index, size='all', iter_size=1000):
        begin_time = datetime.datetime.now()
        whole_size = 0

        logger.info('*' * 80)
        logger.info("Loading index '" + index + "' from server " + self._server)

        # get the size of index
        index_size = self._client.search(
                index=index,
                size=0,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )['hits']['total']

        logger.info("There are %i samples in index '" % index_size + index + "'.")

        if size == 'all':
            whole_size = index_size
        elif int(size) > index_size:
            logger.warning("user defined size is too large(greater than the whole size of index), " +
                           "use the index size instead.")
            whole_size = index_size
        else:
            whole_size = int(size)

        logger.info("load first %i samples into pandas dataframe." % whole_size)

        logger.info("-" * 80)
        logger.info("Loading process begins...")
        pd_result = DataFrame()
        if whole_size <= iter_size:
            page = self._client.search(
                index=index,
                size=whole_size,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )

            pd_result = read_json(json.dumps(page['hits']['hits']))

        else:
            page = self._client.search(
                index=index,
                scroll='2m',
                # search_type = 'scan',
                size=iter_size,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )

            sid = page['_scroll_id']
            scroll_size = whole_size
            #print("scroll_size = %i" % scroll_size)

            pd_result = read_json(json.dumps(page['hits']['hits']))
            _iter = 1
            num_iters = int(whole_size / iter_size) + 1

            while scroll_size > 0:
                logger.debug("Scrolling... %i / %i" % (_iter, num_iters))
                _iter = _iter + 1
                page = self._client.scroll(scroll_id=sid, scroll='2m')
                # Update the scroll ID
                sid = page['_scroll_id']
                # Get the number of results that we returned in the last scroll
                scroll_size = scroll_size - iter_size

                pd_result = concat([pd_result, read_json(json.dumps(page['hits']['hits']))])


                ##print("scroll size: " + str(scroll_size))
        end_time = datetime.datetime.now()

        logger.info('-' * 80)
        logger.info("Loading process ends...")
        logger.info("%i samples loaded in dataframe." % len(pd_result))
        logger.info("Loading process takes " + str(end_time - begin_time))

        return pd_result

