#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
from pymongo.operations import UpdateMany, InsertOne
from pymongo import MongoClient


# In[1]:


def connect_mongodb_collection(host, port, database, collection):
    mongo_client = MongoClient(host, port)
    db = mongo_client[database]
    collection = db[collection]
    return collection


# In[ ]:


mention_collection = connect_mongodb_collection(host='tp-hadoop-51', port=27017, database='gdelt', collection='mention')


# In[ ]:


df = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
                 delimiter = " ",
                 header= None,
                 names = ["ID1","ID2","URL"]
                )


# In[ ]:


# extraction de toutes les url des evenement de 2022
df_event_list = df[df.URL.str.contains('202201[0-9]{8}.*export.CSV',regex= True, na=False)]

#pour chaque fichier de mention on charge le document CSV
for i, event_url in enumerate(df_event_list.URL):
    
    # Chargements des données des mentions
    event_colnames = "GLOBALEVENTID;SQLDATE;MonthYear;Year;FractionDate;Actor1Code;Actor1Name;Actor1CountryCode;Actor1KnownGroupCode;Actor1EthnicCode;Actor1Religion1Code;Actor1Religion2Code;Actor1Type1Code;Actor1Type2Code;Actor1Type3Code;Actor2Code;Actor2Name;Actor2CountryCode;Actor2KnownGroupCode;Actor2EthnicCode;Actor2Religion1Code;Actor2Religion2Code;Actor2Type1Code;Actor2Type2Code;Actor2Type3Code;IsRootEvent;EventCode;EventBaseCode;EventRootCode;QuadClass;GoldsteinScale;NumMentions;NumSources;NumArticles;AvgTone;Actor1Geo_Type;Actor1Geo_FullName;Actor1Geo_CountryCode;Actor1Geo_ADM1Code;Actor1Geo_ADM2Code;Actor1Geo_Lat;Actor1Geo_Long;Actor1Geo_FeatureID;Actor2Geo_Type;Actor2Geo_FullName;Actor2Geo_CountryCode;Actor2Geo_ADM1Code;Actor2Geo_ADM2Code;Actor2Geo_Lat;Actor2Geo_Long;Actor2Geo_FeatureID;ActionGeo_Type;ActionGeo_FullName;ActionGeo_CountryCode;ActionGeo_ADM1Code;ActionGeo_ADM2Code;ActionGeo_Lat;ActionGeo_Long;ActionGeo_FeatureID;DATEADDED;SOURCEURL".split(";")
    event_cols_to_keep = ['GLOBALEVENTID', 'SQLDATE', 'Actor1Geo_CountryCode', 'Actor2Geo_CountryCode', 'ActionGeo_CountryCode']
    df_event_data = pd.read_csv(event_url, sep="\t", names=event_colnames, usecols= event_cols_to_keep, header=None, encoding='latin')
        
    #Supprime les lignes dupliquées
    df_event_data = df_event_data.drop_duplicates()
    
    #nettoyage des df
    df_event_data.SQLDATE = df_event_data.SQLDATE.astype(str)
    
    #on charge les données dans la base
    json_event = df_event_data.to_dict('records')
    
    requests = []
    for event in json_event:
        #mention_collection.update_many({"GLOBALEVENTID": event["GLOBALEVENTID"]}, {"$set" : {"event":event}})
        requests.append(UpdateMany({"GLOBALEVENTID": event["GLOBALEVENTID"]}, {"$set" : {"event":event}}))
    mention_collection.bulk_write(requests, ordered=False)
    
    if i % 500 == 0:
        print(event_url)
        

