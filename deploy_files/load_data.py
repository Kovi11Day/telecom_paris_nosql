#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
import wget
from zipfile import ZipFile
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkFiles
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, MapType
from pyspark.sql.functions import udf
import pandas as pd


# In[83]:


#!pip install wget


# TODO:
# - delete csv option
# - delete zips option
# - create history files if they do not exist
# - add columns to data
# - single file option for tests

# In[2]:


# global variables
MASTER_FILELIST_FILEPATH = 'masterfilelist-translation.txt'
DOWNLOAD_CSV_PATH = './gdelt_data/'
START_URL = 'http://data.gdeltproject.org/gdeltv2/'
HISTORY_EXTRACTED_FILEPATH = 'history_extracted'
HISTORY_LOADED_FILEPATH = 'history_loaded'
EVENTS_COLUMN_HEADERS = './gdelt_columns/events_column_headers'
YEAR = '2022'
MONTH = '06'


# ## get urls from master filelist

# In[3]:


# WARNING WHY ARE SOME URLS MISSING???

def get_zip_urls_from_master_filelist(year_list, month_list, day_list, zip_type, master_filelist_path='masterfilelist-translation.txt', start_url='http://data.gdeltproject.org/gdeltv2/'):
    
    # zip_type: events | mentions | gkg
    zip_type_token = ''
    if zip_type == 'events':
        zip_type_token = '.export.'
    elif zip_type == 'mentions':
        zip_type_token = '.mentions.'
    elif zip_type == 'gkg':
        zip_type_token = '.gkg.'
    else:
        raise Exception('zip_type should be one of: events | mentions | gkg')
        
    # get masterfile list path
    with open(master_filelist_path) as f:
        raw_file_list = f.readlines()
    
    raw_file_list = [line.split() for line in raw_file_list]
    
    # extract zip urls from masterfile list path
    zip_urls = []
    for i in range(len(raw_file_list)):
        try:
            zip_urls.append(raw_file_list[i][2])
        except Exception:  
            pass
        
        
    # filter specified year, month and day
    filtered_zip_urls = []
    
    for year in year_list:
        for month in month_list:
            if day_list is None:
                filtered_zip_urls = filtered_zip_urls + [file for file in zip_urls if (start_url + year + month in file) and (zip_type_token in file)]
            else:
                for day in day_list:
                    filtered_zip_urls = filtered_zip_urls + [file for file in zip_urls if (start_url + year + month + day in file) and (zip_type_token in file)]

                
    return filtered_zip_urls


# ## download and extract urls

# In[4]:


def download_and_extract(zip_urls, download_zip_path, start_url='http://data.gdeltproject.org/gdeltv2/'):
    
    extracted_filenames = []
    
    # create download path of it does not exist
    if not os.path.exists(download_zip_path):
        os.makedirs(download_zip_path)

    for zip_url in zip_urls:
        
        downloaded_filename = zip_url.replace(start_url,'')
        
        # do not downloaded if already exists
        already_downloaded = os.path.exists(download_zip_path+downloaded_filename)
    
        # download zip file
        if not already_downloaded:
            wget.download(zip_url, out=download_zip_path+downloaded_filename)
        extracted_filename = downloaded_filename.replace('.zip','')
    
        # do not unzip if already exists
        already_extracted = os.path.exists(download_zip_path+extracted_filename)
    
        # unzip file
        if not already_extracted:
            with ZipFile(download_zip_path+downloaded_filename, 'r') as zip_ref:
                zip_ref.extractall(download_zip_path)    
        
        # delete zip file
        os.remove(download_zip_path+downloaded_filename)
        
        extracted_filenames = extracted_filenames + [extracted_filename]
    
    return extracted_filenames


# ## data schema

# In[5]:


# EVENTS SCHEMA
# StructField(field_name, field_type, nullable)
events_schema = StructType([
    
    StructField("events_GLOBALEVENTID", StringType(), True),
    StructField("SQLDATE", StringType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    
    StructField("FractionDate", FloatType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    
    StructField("AvgTone", FloatType(), True),
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", IntegerType(), True),
    
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    
    StructField("DATEADDED", IntegerType(), True),
    StructField("SOURCEURL", StringType(), True)
])


# In[6]:


# MENTIONS SCHEMA
# StructField(field_name, field_type, nullable)
mentions_schema = StructType([
    
    StructField("mentions_GLOBALEVENTID", StringType(), True),
    StructField("EventTimeDate", IntegerType(), True),
    StructField("MentionTimeDate", IntegerType(), True),
    StructField("MentionType", IntegerType(), True),
    StructField("MentionSourceName", StringType(), True),
    
    StructField("MentionIdentifier", StringType(), True),
    StructField("SentenceID", IntegerType(), True),
    StructField("Actor1CharOffset", IntegerType(), True),
    StructField("Actor2CharOffset", IntegerType(), True),
    StructField("ActionCharOffset", IntegerType(), True),
    
    StructField("InRawText", IntegerType(), True),
    StructField("Confidence", IntegerType(), True),
    StructField("MentionDocLen", IntegerType(), True),
    StructField("MentionDocTone", FloatType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True),
    
    StructField("Extras", StringType(), True)    
])


# In[7]:


SCHEMA_DICTIONARY = {
    'events':events_schema,
    'mentions':mentions_schema,
    'gkg':None
}


# ## spark session

# In[8]:


# create spark session
SPARK = SparkSession.builder.master('local')     .appName('SparkSession')     .config("spark.mongodb.read.connection.uri", "mongodb://tp-hadoop-50/")     .config("spark.mongodb.write.connection.uri", "mongodb://tp-hadoop-50/")     .getOrCreate()


# ## spark_read_csv

# In[9]:


def spark_read_csv(spark_session, csv_filepath, csv_file_list, schema_dictionary, csv_type):

    df_read = None
    
    for file in csv_file_list:
        
        # file_type: events | mentions | gkg
        csv_type_token = ''
        schema = None
        
        if csv_type == 'events':
            csv_type_token = '.export.'
            schema = schema_dictionary['events']
        elif csv_type == 'mentions':
            csv_type_token = '.mentions.'
            schema = schema_dictionary['mentions']
        elif csv_type == 'gkg':
            csv_type_token = '.gkg.'
            schema = schema_dictionary['gkg']
        else:
            raise Exception('csv_type should be one of: events | mentions | gkg')
        
        # read csv
        df = spark_session.read.options(delimiter='\t').csv(csv_filepath+file, schema=schema)
        
        if df_read is None:
            df_read = df.select('*')
        else:
            df_read = df_read.unionByName(df)
        
    return df_read


# ## transform events data

# In[20]:


def transform_events_data(events_df):
    
    events_columns = ['events_GLOBALEVENTID', 'SQLDATE', 'ActionGeo_CountryCode']
    transformed_df = events_df.select(events_columns)
    
    return transformed_df


# ## write spark dataframe to mongodb collection

# In[11]:


def load_mongodb(spark_dataframe, mongodb_database, mongodb_collection):
    spark_dataframe.write.format('mongodb').option("database",mongodb_database).option("collection", mongodb_collection).mode("append").save()
    


# ## ETL EVENTS DATA

# In[12]:


def etl_events(year_list, month_list, day_list, schema_dictionary, spark_session, mongodb_database, mongodb_collection, download_csv_path, start_url):
    
    # get events urls from master filelist
    zip_urls = get_zip_urls_from_master_filelist(year_list=year_list, month_list=month_list, day_list=day_list, zip_type='events')

    # download and extract csv
    extracted_csvs = download_and_extract(zip_urls, download_zip_path=download_csv_path, start_url=start_url)

    # read csv to spark dataframe
    events_df = spark_read_csv(spark_session=spark_session, csv_filepath=download_csv_path, csv_file_list=extracted_csvs, schema_dictionary=schema_dictionary, csv_type='events')

    # transform events data
    events_df = transform_events_data(events_df)
    
    # load events data to mongodb
    load_mongodb(events_df, mongodb_database=mongodb_database, mongodb_collection=mongodb_collection)
    
    # delete zip file
    os.remove(download_zip_path+extracted_csvs)
    


# In[13]:


etl_events(year_list=['2022'], month_list=['01'], day_list=['01','02','03'], schema_dictionary=SCHEMA_DICTIONARY, spark_session=SPARK, mongodb_database='test', mongodb_collection='events', download_csv_path='./gdelt_data/', start_url='http://data.gdeltproject.org/gdeltv2/')


# In[30]:


def transform_mentions_data(spark_session, mentions_df, mongodb_events_collection, mongodb_database):
    
    # filter mentions columns
    mentions_columns = ['mentions_GLOBALEVENTID', 'MentionIdentifier', 'MentionDocTranslationInfo']
    mentions_columns = ['mentions_GLOBALEVENTID', 'MentionIdentifier']
    
    mentions_df = mentions_df.select(mentions_columns)
    
    # find event GLOBALEVENTID from mongodb events collection
    events_df = spark_session.read.format("mongodb").option("database",mongodb_database).option("collection", mongodb_events_collection).load()
    
    # join events and mentions
    mentions_events_df = mentions_df.join(events_df, events_df.events_GLOBALEVENTID == mentions_df.mentions_GLOBALEVENTID, 'left')
    
    # create nested mentions df
    build_nested_event_udf = udf(lambda SQLDATE, ActionGeo_CountryCode: {
        'SQLDATE': SQLDATE,
        'ActionGeo_CountryCode': ActionGeo_CountryCode
    }, MapType(StringType(), StringType()))

    mentions_events_df = (
        mentions_events_df
        .withColumn('event_fields', build_nested_event_udf(mentions_events_df['SQLDATE'], mentions_events_df['ActionGeo_CountryCode']))
        .drop('SQLDATE')
        .drop('ActionGeo_CountryCode')
        .drop('events_GLOBALEVENTID')
    )
    
    return mentions_events_df


# In[ ]:


def etl_mentions(year_list, month_list, day_list, schema_dictionary, spark_session, mongodb_database, mongodb_mentions_collection, mongodb_events_collection, download_csv_path, start_url):
    
    # get events urls from master filelist
    zip_urls = get_zip_urls_from_master_filelist(year_list=year_list, month_list=month_list, day_list=day_list, zip_type='mentions')

    # download and extract csv
    extracted_csvs = download_and_extract(zip_urls, download_zip_path=download_csv_path, start_url=start_url)

    # read csv to spark dataframe
    mentions_df = spark_read_csv(spark_session, csv_filepath=download_csv_path, csv_file_list=extracted_csvs, schema_dictionary=schema_dictionary, csv_type='mentions')

    # transform events data
    mentions_df = transform_mentions_data(spark_session=spark_session, mentions_df=mentions_df, mongodb_events_collection=mongodb_events_collection, mongodb_database=mongodb_database)
    mentions_df.show()
    # load events data to mongodb
    #load_mongodb(mentions_df, mongodb_database=mongodb_database, mongodb_collection=mongodb_mentions_collection)
    mentions_df.write.format('mongodb').option("database",mongodb_database).option("collection", mongodb_collection).mode("append").save()


# In[ ]:


#etl_mentions(year_list=['2022'], month_list=['01'], day_list=['01','02','03'], schema_dictionary=SCHEMA_DICTIONARY, spark_session=SPARK, mongodb_database='test', mongodb_mentions_collection='mentions', mongodb_events_collection='events', download_csv_path='./gdelt_data/', start_url='http://data.gdeltproject.org/gdeltv2/')


# In[ ]:


#zip_urls = get_zip_urls_from_master_filelist(year_list=['2022'], month_list=['01'], day_list=['01','02','03'], zip_type='mentions')

