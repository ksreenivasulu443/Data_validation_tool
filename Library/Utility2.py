from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()

# Count validation

source = read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee.csv", spark=spark)
source.show()
#print("#"*40)
target = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp """,driver='oracle.jdbc.driver.OracleDriver', spark=spark)
target.show()
#target= read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee_t.csv", spark=spark)

def data_compare( source, target,keycolumn):
    for colname in source.columns:
        source = source.withColumn(colname, trim(col(colname)))

    for colname in target.columns:
        target = target.withColumn(colname, trim(col(colname)))
    columnList = source.columns
    for column in columnList:
        if column not in keycolumn:
            temp_source= source.select(keycolumn, column).withColumnRenamed(column,"source_"+column)
            temp_target=target.select(keycolumn, column).withColumnRenamed(column,"target_"+column)
            temp_join = temp_source.join(temp_target,keycolumn,how='full_outer')
            temp_join.withColumn("comparison", when(col('source_'+column) == col("target_"+column),\
                                                    "True" ).otherwise("False")).show()


data_compare(source, target,'EMPNO')