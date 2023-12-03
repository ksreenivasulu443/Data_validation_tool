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

from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode

#Spark session creation
spark = SparkSession.builder \
    .master("local") \
    .config("spark.jars", '/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/hadoop-azure-3.3.6.jar') \
    .getOrCreate()

#Reading source1

file= read_file("csv","/Users/harish/Desktop/ETL Automation/Contact_info.csv",spark)
file2 = read_file("csv",'/Users/harish/Desktop/Sourcedata2.csv',spark)


file2.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "contact_info_raw") \
    .option("user", "postgres") \
    .option("password", "Dharmavaram1@") \
    .option("driver", 'org.postgresql.Driver') \
    .save()


file3 = file.union(file2)
file.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/freepdb1") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_raw") \
    .option("user", "scott") \
    .option("password", "tiger") \
    .save()

file.createOrReplaceTempView("file")

contact_info_bronze = spark.sql(
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
    Email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from file
    """
)

contact_info_bronze.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/freepdb1") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_bronze") \
    .option("user", "scott") \
    .option("password", "tiger") \
    .save()

contact_info_bronze.createOrReplaceTempView("contact_info_bronze")
contact_info_bronze.show()

contact_info_silver= spark.sql(
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

contact_info_silver.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/freepdb1") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "contact_info_silver") \
    .option("user", "scott") \
    .option("password", "tiger") \
    .save()