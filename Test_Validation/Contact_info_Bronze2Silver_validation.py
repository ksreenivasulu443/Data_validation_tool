import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from Library.General_Purpose_Functions import count_validation,duplicate , \
     Null_value_check,Uniquess_check,records_present_only_in_source,\
     records_present_only_in_target, data_compare,compare


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

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


print(config_file_data['contact_info'])


db_Address = config_file_data['contact_info']['db_Address']
db_Username= config_file_data['contact_info']['db_Username']
db_Password= config_file_data['contact_info']['db_Password']
db_driver= config_file_data['contact_info']['db_driver']
raw_query = config_file_data['contact_info']['raw_query']
bronze_query = config_file_data['contact_info']['bronze_query']
silver_query= config_file_data['contact_info']['silver_query']
Key_column = ['Identifier','Current_ind']
#Key_column = ['Identifier']
unique_col =['Identifier']

#Reading source1

bronze= db_read(db_Address,db_Username,db_Password,bronze_query,db_driver,spark)
contact_info_silver_actual= db_read(db_Address,db_Username,db_Password,silver_query,db_driver,spark)

bronze.createOrReplaceTempView("contact_info_bronze")


contact_info_silver_expected= spark.sql(
        """
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number,
        primary_street_name,
        city,
        state,
        zipcode,
        Email,
        Phone,
        birthmonth,
        'Y' as Current_ind
        from contact_info_bronze 
        union 
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number_prev,
        primary_street_name_prev,
        city_prev,
        state_prev,
        zipcode_prev,
        Email,
        Phone,
        birthmonth,
        'N' as Current_ind
        from contact_info_bronze
        """)


count_validation(contact_info_silver_expected,contact_info_silver_actual,Out=Out)
duplicate(contact_info_silver_actual,Key_column,Out=Out)
records_present_only_in_source(contact_info_silver_expected,contact_info_silver_actual,Key_column,Out)
records_present_only_in_target(contact_info_silver_expected,contact_info_silver_actual,Key_column,Out)
Null_value_check(contact_info_silver_actual,Key_column,Out)
Uniquess_check(contact_info_silver_actual,unique_col,Out)
#data_compare(contact_info_silver_expected,contact_info_silver_actual,'Identifier',Out)
compare(contact_info_silver_expected,contact_info_silver_actual,10,Key_column,Out)

Summary = pd.DataFrame(Out)


Summary = spark.createDataFrame(Summary)
Summary.show()
Summary.write.csv("/Users/harish/PycharmProjects/Data_validation_tool/Output/Summary", mode='overwrite', header="True")
