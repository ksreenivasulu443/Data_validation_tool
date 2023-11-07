from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()

# Count validation

source = read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee.csv", spark=spark)
source.show()
#print("#"*40)
#target = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp1 """,driver='oracle.jdbc.driver.OracleDriver')
#target.show()
target= read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee_t.csv", spark=spark)

print( source.columns)
for i in source.columns:
    print(i)