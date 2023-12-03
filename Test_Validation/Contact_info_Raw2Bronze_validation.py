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

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


print(config_file_data['contact_info'])

path= config_file_data['contact_info']['source_file']
format = config_file_data['contact_info']['source_file_type']
db_Address = config_file_data['contact_info']['db_Address']
db_port= config_file_data['contact_info']['db_Port']
db_Username= config_file_data['contact_info']['db_Username']
db_Password= config_file_data['contact_info']['db_Password']
db_driver= config_file_data['contact_info']['db_driver']
raw_query = config_file_data['contact_info']['raw_query']
bronze_query = config_file_data['contact_info']['bronze_query']
silver_query= config_file_data['contact_info']['silver_query']
Key_column = config_file_data['contact_info']['Key_column']

#Reading source1

Raw= db_read(db_Address,db_Username,db_Password,raw_query,db_driver,spark)
contact_info_bronze_actual= db_read(db_Address,db_Username,db_Password,bronze_query,db_driver,spark)

Raw.createOrReplaceTempView("Raw")

contact_info_bronze_expected = spark.sql(
    """ select
    cast(Identifier as decimal(10)) Identifier,
    upper(Surname) Surname,
    upper(given_name) given_name,
    upper(middle_initial) middle_initial,
    suffix,
    Primary_street_number,
    primary_street_name,
    city,
    state,
    zipcode,
    Primary_street_number_prev,
    primary_street_name_prev,
    city_prev,
    state_prev,
    zipcode_prev,
    translate(Email,'FUCK','****') email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from raw
    """
)


count_validation(contact_info_bronze_expected,contact_info_bronze_actual,Out=Out)
duplicate(contact_info_bronze_actual,Key_column,Out=Out)
records_present_only_in_source(contact_info_bronze_expected,contact_info_bronze_actual,Key_column,Out)
records_present_only_in_target(contact_info_bronze_expected,contact_info_bronze_actual,Key_column,Out)
Null_value_check(contact_info_bronze_actual,Key_column,Out)
Uniquess_check(contact_info_bronze_actual,Key_column,Out)
data_compare(contact_info_bronze_expected,contact_info_bronze_actual,'Identifier',Out)
Summary = pd.DataFrame(Out)


Summary = spark.createDataFrame(Summary)
Summary.show()
Summary.write.csv("/Users/harish/PycharmProjects/Data_validation_tool/Output/Summary", mode='overwrite', header="True")

Summary.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/freepdb1") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_raw") \
    .option("user", "scott") \
    .option("password", "tiger") \
    .save()