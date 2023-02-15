#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from pymongo.operations import UpdateMany, InsertOne
from pymongo import MongoClient


# In[3]:


def connect_mongodb_collection(host, port, database, collection):
    mongo_client = MongoClient(host, port)
    db = mongo_client[database]
    collection = db[collection]
    return collection


# In[4]:


mention_collection = connect_mongodb_collection(host='tp-hadoop-50', port=27017, database='final', collection='mention2')


# In[11]:


df = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
                 delimiter = " ",
                 header= None,
                 names = ["ID1","ID2","URL"]
                )


# In[12]:


#%%time

# extraction de toutes les url des mentions de janvier à mars 2022
df_mention_list = df[df.URL.str.contains('202203[0-9]{8}.*mentions.CSV',regex= True, na=False)]

#pour chaque fichier de mention on charge le document CSV
for i, mention_url in enumerate(df_mention_list.URL):
            
    # Chargements des données des mentions
    mention_colnames = ["GLOBALEVENTID","EventTimeDate","MentionTimeDate","MentionType","MentionSourceName","MentionIdentifier","SentenceID","Actor1CharOffset","Actor2CharOffset","ActionCharOffset","InRawText","Confidence","MentionDocLen","MentionDocTone","MentionDocTranslationInfo","Extras"]
    mention_cols_to_keep = ['GLOBALEVENTID', 'MentionTimeDate', 'MentionIdentifier']
    df_mention_data = pd.read_csv(mention_url, sep="\t", names=mention_colnames, usecols=mention_cols_to_keep, header=None, encoding='latin')
    
    #Supprime les lignes dupliquées
    df_mention_data = df_mention_data.drop_duplicates()
    
    #on charge les données dans la base
    json_mention = df_mention_data.to_dict('records')
    #mention_collection.insert_many(json_mention)
    requests = []
    for m in json_mention:
        requests.append(InsertOne(m))
    mention_collection.bulk_write(requests, ordered=False)
    
    if i % 500 == 0:
        print(mention_url)


# In[ ]:




