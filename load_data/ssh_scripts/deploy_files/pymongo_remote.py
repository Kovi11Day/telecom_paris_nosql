#!/usr/bin/env python
# coding: utf-8

# In[6]:


#! pip install sshtunnel
#! pip install pymongo


# In[12]:


import pandas as pd
import numpy as np

from sshtunnel import SSHTunnelForwarder
import pymongo
from pymongo.operations import UpdateMany, InsertOne
from pymongo import MongoClient


# ### Classe de connection à la base MongoDB

# In[4]:


class Connection_MongoDB:

    def __init__(self, bridge_host="137.194.211.146", bridge_username="ubuntu", remote_server_name="tp-hadoop-50", remote_server_port=27017):
        self.server = SSHTunnelForwarder(
            bridge_host,
            ssh_username=bridge_username,
            remote_bind_address=(remote_server_name,remote_server_port)
        ) 
        self.server.start()
        self.session = pymongo.MongoClient(self.server.local_bind_hosts[0], self.server.local_bind_port)
    
    def __del__(self):
        self.server.stop()
        
    def get_collection(self, db_name = "test", collection_name="events"):
        db = self.session[db_name]
        self.collection = db[collection_name]
        return self
    
    def find(self, request):
        return self.collection.find(request)
    
    def aggregate(self, request):
        return self.collection.aggregate(request)
    
    def insert_many(self, request):
        return self.collection.insert_many(request)
    
    def remove_all(self):
        return self.collection.delete_many( { } )
    
    def create_index(self, fields, unique=True):
        return self.collection.create_index(fields, unique=unique)
    
    def update_many(self, filter_request, update_request):
        return self.collection.update_many(filter_request, update_request)
    
    def bulk_write(self, requests, ordered=False):
        return self.collection.bulk_write(requests, ordered=ordered)
    
    
#events = Connection_MongoDB().get_collection()
#result = events.find({"SQLDATE": "20211225"})
#list_cur = list(result)
#pd.DataFrame(list_cur)


# ### Chargement des données dans la base

# In[3]:


#mention_collection = Connection_MongoDB().get_collection(db_name = "test", collection_name="gdelt")


# In[13]:


mongo_client = MongoClient('tp-hadoop-50', 27017)
db = mongo_client.test
mention_collection = db.gdelt


# In[4]:


df = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
                 delimiter = " ",
                 header= None,
                 names = ["ID1","ID2","URL"]
                )


# In[ ]:


#%%time

# extraction de toutes les url des mentions de janvier à mars 2022
df_mention_list = df[df.URL.str.contains('20220101[0-9]{6}.*mentions.CSV',regex= True, na=False)]

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

