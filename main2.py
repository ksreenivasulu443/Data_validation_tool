from Library.Database_Read_Functions import db_read
from Library.File_Read_functions import read_file
from Library.General_Purpose_Functions import count_validation,duplicate , \
     Null_value_check,Uniquess_check,records_present_only_in_source,\
     records_present_only_in_target, data_compare

from pyspark.sql import SparkSession
import pandas as pd
import json
import openpyxl
from pyspark.sql.functions import collect_set

jar_path='/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/sqljdbc4-2.0.jar'
spark = SparkSession.builder.master("local").appName("test_execution")\
     .config("spark.jars", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.driver.extraClassPath", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.executor.extraClassPath","/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .getOrCreate()

# df= spark.read.csv("/Users/harish/PycharmProjects/AutomationSept2/Config17.csv", header=True,sep=",")
# df.show()

Test_cases = pd.read_excel("/Users/harish/PycharmProjects/Data_validation_tool/Config/Master_Test_Template.xlsx")
print(Test_cases.shape)#

print(Test_cases.columns)
Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


df = spark.createDataFrame(Test_cases)

group_user = df.groupBy('Source_file_info','Source_Database_info','Source_Query',
                        'Target_file_info',	'Target_Database_info',"Target_Query").agg(collect_set('Validation_Type').alias('Validation_Type'))

group_user.show(truncate=False)
group_user = group_user.toPandas()



with open(r'config/config.json','r') as f:
    config_file_data = json.loads(f.read())

for key,value in group_user.iterrows():
     Source_file_info= value['Source_file_info']
     Source_Database_info = value['Source_Database_info']
     Target_file_info = value['Target_file_info']
     Target_Database_info= value['Target_Database_info']
     validations= value['Validation_Type']
     source_query = value['Source_Query']
     if Source_file_info is not None:
          source_info= config_file_data[Source_file_info]
          path=source_info['path']
          format=source_info['file_type']
          source = read_file(format,path, spark)
          source.show()
     else:
          source_info = config_file_data[Source_Database_info]
          db_Address= source_info['db_Address']
          db_Port= source_info['db_Port']
          db_Username= source_info['db_Username']
          db_Password = source_info['db_Password']
          db_Name=  source_info['db_Name']
          db_driver = source_info['db_driver']
          source = db_read(db_Address,db_Username,db_Password,source_query,db_driver,spark)
          source.show()
     if Target_Database_info is not None:
          target_info = config_file_data[Target_Database_info]
          print(target_info)
          db_Address = target_info['db_Address']
          db_Port = target_info['db_Port']
          db_Username = target_info['db_Username']
          db_Password = target_info['db_Password']
          db_Name = target_info['db_Name']
          db_driver = target_info['db_driver']
          target = db_read(db_Address, db_Username, db_Password, source_query, db_driver, spark)
          target.show()
     else:
          target_info= config_file_data[Target_file_info]
          path=target_info['path']
          format=target_info['file_type']
          target = read_file(format,path, spark)
          target.show()

     for i in validations:
          print(i)
          if i == 'count_validation' :
               count_validation(source, target,Out)
          elif i =='duplicate':
               duplicate(target, ['Empno'],Out)
          elif i == 'Null_value_check':
               Null_value_check(target,['Empno','comm'],Out)
          elif i =='Uniquess_check':
               Uniquess_check(target,['Empno'],Out)

          elif i == 'records_present_only_in_source':
               records_present_only_in_source(source, target,['Empno'],Out)

          elif i == 'records_present_only_in_target':
               records_present_only_in_target(source, target,['Empno'],Out)

          elif i == 'data_compare':
               data_compare(source,target,'EMPNO',Out)

Summary = pd.DataFrame(Out)
# This code comment
# This comment2

Summary = spark.createDataFrame(Summary)
Summary.show()
Summary.write.csv("/Users/harish/PycharmProjects/Data_validation_tool/Output/Summary", mode='append', header="True")
