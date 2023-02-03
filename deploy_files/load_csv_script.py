#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkFiles
import requests
import pandas as pd

# In[ ]:

test = pd.DataFrame()
test_data_path = 'test.csv'
spark = SparkSession.builder.master('local').appName('SparkSession').getOrCreate()
df = spark.read.csv(test_data_path)
df.show()
