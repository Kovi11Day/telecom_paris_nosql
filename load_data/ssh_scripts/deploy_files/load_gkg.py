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


mention_collection = connect_mongodb_collection(host='tp-hadoop-51', port=27017, database='final', collection='mention2')


# In[ ]:


df = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
                 delimiter = " ",
                 header= None,
                 names = ["ID1","ID2","URL"]
                )


# In[ ]:


# extraction de toutes les url des articles gkg de 2022
df_gkg_list = df[df.URL.str.contains('202201[0-9]{8}.*gkg.csv',regex= True, na=False)]


#pour chaque fichier de mention on charge le document CSV
for i, gkg_url in enumerate(df_gkg_list.URL):
    
    # Chargements des données des mentions
    gkg_colnames = ["GKGRECORDID","DATE","SourceCollectionIdentifier","SourceCommonName","DocumentIdentifier","Counts","V2Counts","Themes","V2Themes","Locations","V2Locations","Persons","V2Persons","Organizations","V2Organizations","V2Tone","Dates","GCAM","SharingImage","RelatedImages","SocialImageEmbeds","SocialVideoEmbeds","Quotations","AllNames","Amounts","TranslationInfo","Extras"]
    gkg_cols_to_keep = ['GKGRECORDID', 'DATE', 'SourceCommonName', 'DocumentIdentifier', 'Themes', 'Persons', 'V2Tone', "TranslationInfo"]
    df_gkg_data = pd.read_csv(gkg_url, sep="\t", names=gkg_colnames, usecols=gkg_cols_to_keep, header=None, encoding='latin')
    #Supprime les lignes dupliquées
    df_gkg_data = df_gkg_data.drop_duplicates()
    
    #nettoyage des df
    df_gkg_data.TranslationInfo = df_gkg_data.TranslationInfo.str.extract(r'srclc:([a-z]{3})*')
    df_gkg_data.Themes = df_gkg_data.Themes.str.split(";")
    df_gkg_data.Persons = df_gkg_data.Persons.str.split(";")
    df_gkg_data.V2Tone = df_gkg_data.V2Tone.str.split(",").str[0].astype(float)
    
    #on charge les données dans la base
    json_gkg = df_gkg_data.to_dict('records')
    requests = []
    for article in json_gkg:
        #mention_collection.update_many({"MentionIdentifier": article["DocumentIdentifier"]}, {"$set" : {"article":article}})
        requests.append(UpdateMany({"MentionIdentifier": article["DocumentIdentifier"]}, {"$set" : {"article":article}}))
    mention_collection.bulk_write(requests, ordered=False)
    
    if i % 500 == 0:
        print(gkg_url)

