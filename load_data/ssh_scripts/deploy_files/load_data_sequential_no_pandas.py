#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
import wget
from zipfile import ZipFile
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark import SparkFiles
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, MapType
#from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import pandas as pd
from subprocess import PIPE, Popen


# In[83]:


#!pip install wget


# ## data schema

# In[ ]:


# EVENTS SCHEMA
# StructField(field_name, field_type, nullable)
events_schema = StructType([
    
    StructField("GLOBALEVENTID", StringType(), True), #
    StructField("SQLDATE", StringType(), True), #
    StructField("MonthYear", StringType(), True),
    StructField("Year", StringType(), True),
    
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
    StructField("Actor1Geo_CountryCode", StringType(), True), #
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", IntegerType(), True),
    
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True), #
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True), #
    
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    
    StructField("DATEADDED", IntegerType(), True),
    StructField("SOURCEURL", StringType(), True)
])


# In[ ]:


# MENTIONS SCHEMA
# StructField(field_name, field_type, nullable)
mentions_schema = StructType([
    
    StructField("GLOBALEVENTID", StringType(), True), #
    StructField("EventTimeDate", StringType(), True), #
    StructField("MentionTimeDate", StringType(), True), #
    StructField("MentionType", IntegerType(), True),
    StructField("MentionSourceName", StringType(), True),
    
    StructField("MentionIdentifier", StringType(), True), #
    StructField("SentenceID", IntegerType(), True),
    StructField("Actor1CharOffset", IntegerType(), True),
    StructField("Actor2CharOffset", IntegerType(), True),
    StructField("ActionCharOffset", IntegerType(), True),
    
    StructField("InRawText", IntegerType(), True),
    StructField("Confidence", IntegerType(), True),
    StructField("MentionDocLen", IntegerType(), True),
    StructField("MentionDocTone", FloatType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True), #
    
    StructField("Extras", StringType(), True)    
])


# In[ ]:


# GKG SCHEMA
# StructField(field_name, field_type, nullable)
gkg_schema = StructType([
    
    StructField("GKGRECORDID", StringType(), True), #
    StructField("DATE", StringType(), True), #
    StructField("SourceCollectionIdentifier", IntegerType(), True), 
    StructField("SourceCommonName", StringType(), True), #
    StructField("DocumentIdentifier", StringType(), True), #
    
    StructField("Counts", StringType(), True), 
    StructField("V2Counts", StringType(), True),
    StructField("Themes", StringType(), True), #
    StructField("V2Themes", StringType(), True), #
    StructField("Locations", StringType(), True),
    
    StructField("V2Locations", StringType(), True),
    StructField("Persons", StringType(), True), #
    StructField("V2Persons", StringType(), True), #
    StructField("Organizations", StringType(), True),
    StructField("V2Organizations", StringType(), True),
    
    StructField("V2Tone", StringType(), True), # first array element only
    StructField("Dates", StringType(), True),
    StructField("GCAM", StringType(), True),
    StructField("SharingImage", StringType(), True),
    StructField("RelatedImages", StringType(), True),    
    
    StructField("SocialImageEmbeds", StringType(), True),
    StructField("SocialVideoEmbeds", StringType(), True),
    StructField("Quotations", StringType(), True),
    StructField("AllNames", StringType(), True),
    StructField("Amounts", StringType(), True), 
    
    StructField("TranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
    
])


# In[ ]:


SCHEMA_DICTIONARY = {
    'events':events_schema,
    'mentions':mentions_schema,
    'gkg':gkg_schema
}


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


# ## spark session

# In[ ]:


# create spark session
#SPARK = SparkSession.builder.master('local') \

#SPARK = SparkSession.builder \
#   .master('spark://tp-hadoop-57:7077') \

SPARK = SparkSession.builder     .appName('SparkSession')     .config("spark.mongodb.read.connection.uri", "mongodb://tp-hadoop-50/")     .config("spark.mongodb.write.connection.uri", "mongodb://tp-hadoop-50/")     .getOrCreate()

#SC = SparkContext()
#SC = SparkContext.getOrCreate().setMaster("spark://tp-hadoop-51:7077")
SC = SparkContext.getOrCreate()

print(SC)


# ## utility - write to mongodb

# In[ ]:


def load_mongodb(spark_dataframe, mongodb_database, mongodb_collection):
    spark_dataframe.write.format('mongodb').option("database",mongodb_database).option("collection", mongodb_collection).mode("append").save()
    


# ## utility - select columns

# In[ ]:


def select_columns_df(df, df_type):
    
    #events_columns = ['GLOBALEVENTID', 'SQLDATE', 'Actor1Geo_CountryCode', 'Actor2Geo_CountryCode', 'ActionGeo_CountryCode', ]
    #mentions_columns = ['GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate', 'MentionIdentifier',  'MentionDocTranslationInfo']
    #gkg_columns = ['GKGRECORDID', 'DATE', 'SourceCommonName', 'DocumentIdentifier', 'Themes', 'V2Themes', 'Persons', 'V2Persons', 'V2Tone']

    events_columns = ['GLOBALEVENTID', 'SQLDATE', 'Actor1Geo_CountryCode', 'Actor2Geo_CountryCode', 'ActionGeo_CountryCode', ]
    mentions_columns = ['GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate']
    gkg_columns = ['GKGRECORDID', 'DATE', 'SourceCommonName', 'DocumentIdentifier', 'Themes', 'V2Themes', 'Persons', 'V2Persons', 'V2Tone']


    selection_columns = []
    if df_type == 'events':
        selection_columns = events_columns
    elif df_type == 'mentions':
        selection_columns = mentions_columns
    elif df_type == 'gkg':
        selection_columns = gkg_columns
    else:
        raise Exception('df_type must be: events| mentions |gkg')
        
    transformed_df = df.select(selection_columns)
    
    return transformed_df


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


def download_and_extract(spark_context, zip_urls, download_zip_path, start_url='http://data.gdeltproject.org/gdeltv2/'):
    
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
        
        # add file to be available on each node
        print('add file spark context')
        spark_context.addFile(download_zip_path+extracted_filename)
        #spark_context.addFile('file:///home/ubuntu/gdelt_data/'+extracted_filename)
        #put = Popen(["../hadoop-3.3.4/bin/hadoop", "fs", "-put", download_zip_path+extracted_filename, "hdfs://tp-hadoop-57:9000/user/ubuntu/gdelt_data"], stdin=PIPE, bufsize=-1)
        # delete zip file
        os.remove(download_zip_path+downloaded_filename)
        
        extracted_filenames = extracted_filenames + [extracted_filename]
    
    return extracted_filenames


# ## spark_read_csv

# In[10]:


def spark_read_csv(spark_session, spark_context, csv_filepath, csv_file_list, schema_dictionary, csv_type):

    df_read = None
    
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
        
    
    for file in csv_file_list:
        
        # read csv
        #df = spark_session.read.options(delimiter='\t').csv(csv_filepath+file, schema=schema)
        #df = spark_session.read.options(delimiter='\t').csv('file:///home/ubuntu/gdelt_data/'+file, schema=schema)
        #df = spark_session.read.options(delimiter='\t').csv(SparkFiles.get('file:///home/ubuntu/gdelt_data/'+file), schema=schema)
        print('read file from spark context')
        df = spark_session.read.options(delimiter='\t').csv('file:///' + SparkFiles.get(file), schema=schema)
        
        # get masterfile list path

        if df_read is None:
            df_read = df.select('*')
        else:
            df_read = df_read.unionByName(df)
        
    return df_read


# ## LOAD DATA

# In[ ]:


def load_data(year_list, month_list, day_list, schema_dictionary, spark_session, spark_context, mongodb_database, mongodb_collection, download_csv_path, start_url):
    
    # download and extract mentions csv files
    mentions_zip_urls = get_zip_urls_from_master_filelist(year_list=year_list, month_list=month_list, day_list=day_list, zip_type='mentions')
    print("get_zip_urls_from_master_filelist")
    mentions_zip_urls = spark_context.parallelize(mentions_zip_urls)
    mentions_zip_urls.foreach(lambda url: load_data_zip(url, year_list=year_list, 
          month_list=month_list,
          day_list=day_list, 
          schema_dictionary=schema_dictionary, 
          spark_session = spark_session, 
          spark_context = spark_context,
          mongodb_database = mongodb_database,
          mongodb_collection = mongodb_collection, 
          download_csv_path = download_csv_path, 
          start_url = start_url))
    


# In[ ]:


def load_data_zip(url, year_list, month_list, day_list, schema_dictionary, spark_session, spark_context, mongodb_database, mongodb_collection, download_csv_path, start_url):
    
    mentions_extracted_csvs = download_and_extract(spark_context=spark_context, zip_urls=[url], download_zip_path=download_csv_path, start_url=start_url)
    print("download_and_extract")
    
    stop_counter = 2
    counter = 0
    
    for mention_csv_file in mentions_extracted_csvs:
        counter = counter + 1
        if counter == stop_counter:
            break
        # read mentions csv to spark dataframe
        mentions_df = spark_read_csv(spark_session=spark_session, spark_context=spark_context, csv_filepath=download_csv_path, csv_file_list=[mention_csv_file], schema_dictionary=schema_dictionary, csv_type='mentions')
        print("spark_read_csv")
        mentions_df = select_columns_df(mentions_df, df_type='mentions')
        print("select_columns_df")
        
        # GET EVENTS

        print("global_event_id_filter")
        global_event_id_filter = mentions_df.select('GLOBALEVENTID').distinct().rdd.flatMap(lambda x: x).collect()
        
        event_dates = mentions_df.select('EventTimeDate').distinct()
        event_dates = event_dates.withColumn('year', substring('EventTimeDate', 1,4))
        event_dates = event_dates.withColumn('month', substring('EventTimeDate', 5,2))
        event_dates = event_dates.withColumn('day', substring('EventTimeDate', 7,2))

        events_df = None
        
        year = event_dates.first()['year']
        month = event_dates.first()['month']
        day = event_dates.first()['day']
        
        event_zip_urls = get_zip_urls_from_master_filelist(year_list=[year], month_list=[month], day_list=[day], zip_type='events')
        events_extracted_csvs = download_and_extract(spark_context=spark_context, zip_urls=event_zip_urls, download_zip_path=download_csv_path, start_url=start_url)
        row_events_df = spark_read_csv(spark_session=spark_session, spark_context=spark_context, csv_filepath=download_csv_path, csv_file_list=events_extracted_csvs, schema_dictionary=schema_dictionary, csv_type='events')

        row_events_df = row_events_df.filter(row_events_df.GLOBALEVENTID.isin(global_event_id_filter))

        row_events_df = select_columns_df(row_events_df, df_type='events')

        if events_df is None:
            events_df = row_events_df.select('*')
        else:
            events_df = events_df.unionByName(row_events_df)
            
        print("about to join")
        mentions_events_df = mentions_df.join(events_df, mentions_df.GLOBALEVENTID == events_df.GLOBALEVENTID, 'left')
        
        mentions_events_df.show()
        
        # WRITE TO MONGODB
        load_mongodb(mentions_events_df, mongodb_database=mongodb_database, mongodb_collection=mongodb_collection)
        





# In[13]:


def load_data2(year_list, month_list, day_list, schema_dictionary, spark_session, spark_context, mongodb_database, mongodb_collection, download_csv_path, start_url):
    
    # GET MENTIONS
    # download and extract mentions csv files
    mentions_zip_urls = get_zip_urls_from_master_filelist(year_list=year_list, month_list=month_list, day_list=day_list, zip_type='mentions')
    print("get_zip_urls_from_master_filelist")
    mentions_extracted_csvs = download_and_extract(spark_context=spark_context, zip_urls=mentions_zip_urls, download_zip_path=download_csv_path, start_url=start_url)
    print("download_and_extract")
    
    stop_counter = 2
    counter = 0
    
    for mention_csv_file in mentions_extracted_csvs:
        counter = counter + 1
        if counter == stop_counter:
            break
        # read mentions csv to spark dataframe
        mentions_df = spark_read_csv(spark_session=spark_session, spark_context=spark_context, csv_filepath=download_csv_path, csv_file_list=[mention_csv_file], schema_dictionary=schema_dictionary, csv_type='mentions')
        print("spark_read_csv")
        mentions_df = select_columns_df(mentions_df, df_type='mentions')
        print("select_columns_df")
        
        # GET EVENTS

        print("global_event_id_filter")
        global_event_id_filter = mentions_df.select('GLOBALEVENTID').distinct().rdd.flatMap(lambda x: x).collect()
        
        event_dates = mentions_df.select('EventTimeDate').distinct()
        event_dates = event_dates.withColumn('year', substring('EventTimeDate', 1,4))
        event_dates = event_dates.withColumn('month', substring('EventTimeDate', 5,2))
        event_dates = event_dates.withColumn('day', substring('EventTimeDate', 7,2))

        events_df = None
        
        year = event_dates.first()['year']
        month = event_dates.first()['month']
        day = event_dates.first()['day']
        
        event_zip_urls = get_zip_urls_from_master_filelist(year_list=[year], month_list=[month], day_list=[day], zip_type='events')
        events_extracted_csvs = download_and_extract(spark_context=spark_context, zip_urls=event_zip_urls, download_zip_path=download_csv_path, start_url=start_url)
        row_events_df = spark_read_csv(spark_session=spark_session, spark_context=spark_context, csv_filepath=download_csv_path, csv_file_list=events_extracted_csvs, schema_dictionary=schema_dictionary, csv_type='events')

        row_events_df = row_events_df.filter(row_events_df.GLOBALEVENTID.isin(global_event_id_filter))

        row_events_df = select_columns_df(row_events_df, df_type='events')

        if events_df is None:
            events_df = row_events_df.select('*')
        else:
            events_df = events_df.unionByName(row_events_df)
            
        print("about to join")
        mentions_events_df = mentions_df.join(events_df, mentions_df.GLOBALEVENTID == events_df.GLOBALEVENTID, 'left')
        
        mentions_events_df.show()
        
        # WRITE TO MONGODB
        load_mongodb(mentions_events_df, mongodb_database=mongodb_database, mongodb_collection=mongodb_collection)
        


# In[14]:


# started: 23:21-ABORTED
load_data(year_list=['2022'], 
          month_list=['01'],
          day_list=['01'], 
          schema_dictionary=SCHEMA_DICTIONARY, 
          spark_session = SPARK, 
          spark_context = SC,
          mongodb_database = 'test',
          mongodb_collection = 'events', 
          download_csv_path = '../gdelt_data/', 
          start_url = 'http://data.gdeltproject.org/gdeltv2/')


# In[ ]:




