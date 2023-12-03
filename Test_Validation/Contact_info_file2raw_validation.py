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

from Library.File_Read_functions import read_file

from Library.Database_Read_Functions import db_read

from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode

#Spark session creation
spark = SparkSession.builder \
    .master("local") \
    .config("spark.jars", '/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/hadoop-azure-3.3.6.jar') \
    .getOrCreate()

with open('/Users/harish/PycharmProjects/Data_validation_tool/Config/config.json','r') as f:
    config_file_data = json.loads(f.read())

print(config_file_data['contact_info'])

path= config_file_data['contact_info']['source_file']
format = config_file_data['contact_info']['source_file_type']
db_Address = config_file_data['contact_info']['db_Address']
db_port= config_file_data['contact_info']['db_Port']
db_Username= config_file_data['contact_info']['db_Username']
db_Password= config_file_data['contact_info']['db_Password']
db_driver= config_file_data['contact_info']['db_driver']
query = config_file_data['contact_info']['query']
Key_column = config_file_data['contact_info']['Key_column']

#Reading source1

Source1= read_file(format,path,spark)
Target1= db_read(db_Address,db_Username,db_Password,query,db_driver,spark)

Source1.show()
Target1.show()

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


count_validation(Source1,Target1,Out=Out)
duplicate(Target1,Key_column,Out=Out)
records_present_only_in_source(Source1,Target1,Key_column,Out)
records_present_only_in_target(Source1,Target1,Key_column,Out)
Null_value_check(Target1,Key_column,Out)
Uniquess_check(Target1,Key_column,Out)
data_compare(Source1,Target1,'Identifier',Out)
Summary = pd.DataFrame(Out)
# This code comment
# This comment2

Summary = spark.createDataFrame(Summary)
Summary.show()
Summary.write.csv("/Users/harish/PycharmProjects/Data_validation_tool/Output/Summary", mode='append', header="True")
