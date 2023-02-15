#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkFiles
import pandas as pd


# In[4]:


def load_mongodb(spark_dataframe, mongodb_database, mongodb_collection):
    spark_dataframe.write.format('mongodb').option("database",mongodb_database).option("collection", mongodb_collection).mode("append").save()


# In[ ]:


def get_gdelt_headers(events_header_file, mentions_header_file, gkg_header_file):

    gdelt_headers_dict = {}

    # get events headers
    with open(events_header_file) as f:
        events_headers = f.read()
    events_headers = eval(events_headers)
    gdelt_headers_dict['EVENTS_HEADERS'] = events_headers

    # get events headers
    with open(mentions_header_file) as f:
        mentions_headers = f.read()
    mentions_headers = eval(mentions_headers)
    gdelt_headers_dict['MENTIONS_HEADERS'] = mentions_headers

    # get gkg headers
    with open(gkg_header_file) as f:
        gkg_headers = f.read()
    gkg_headers = eval(gkg_headers)
    gdelt_headers_dict['GKG_HEADERS'] = gkg_headers

    return gdelt_headers_dict

if __name__ == "__main__":
    test_data_path = 'file:///home/ubuntu/deploy_files/test.csv'
    spark = SparkSession.builder.appName('SparkSession').getOrCreate()
    df = spark.read.csv(test_data_path)
    df.show()
    load_mongodb(df, mongodb_database='test', mongodb_collection='events')
