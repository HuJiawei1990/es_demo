# -*- coding: utf-8 -*-

"""
<file_name>
---
#author	    hjw
#date		2017-07-26
#version	0.0.1.20170726
---
<enter your description here>
"""

import es_connector
import datetime
import json
import logging


"""
Logging configuration
"""


"""
ES Server configuration
"""
##server12 = es_connector.es_server(ip='172.31.18.12', port='9200')
server33 = es_connector.es_server(ip='172.31.18.33', port='9200')
##server12.get_version()

begin_time = datetime.datetime.now()
##df1 = server12.load_data_to_pd(index='server-metrics')
df1 = server33.load_data_to_pd(index='sdf', size=1000)
end_time = datetime.datetime.now()

print('*' * 50)
print(df1.head(10))
print('length of data = %i' % len(df1))


