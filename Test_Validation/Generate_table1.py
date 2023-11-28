import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from Library.General_Purpose_Functions import count_validation,duplicate , \
     Null_value_check,Uniquess_check,records_present_only_in_source,\
     records_present_only_in_target, data_compare

from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode

#Spark session creation
spark = SparkSession.builder \
    .master("local") \
    .config("spark.jars", '/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/hadoop-azure-3.3.6.jar') \
    .getOrCreate()

#Reading Config details

storage_account = 'teststorage454'
container_name_1 = 'sreeni'
account_key = 'xeiCLA3E5eum+T2sNJJTXs1JLSdHl33+kUucI8RrGAWTbV94alkgYcsEY5JF+Jp4GN7zbltLjjXt+AStNewhLg=='




spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", account_key)

target = spark.read.format("csv")\
    .load(f"abfss://"+container_name_1+"@"+storage_account+".dfs.core.windows.net/sreeni/train_and_test2.csv")

