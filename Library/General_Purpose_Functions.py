from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()

# Count validation

source = read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee.csv", spark=spark)
#source.show()
print("#"*40)
target = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp1 """,driver='oracle.jdbc.driver.OracleDriver')
#target.show()



def count_validation(sourceDF, targetDF):
    source_count = sourceDF.count()
    target_count = targetDF.count()
    if source_count == target_count:
        print("Source count and target count is matching and count is", source_count)
    else:
        print("Source count and taget count is not matching and difference is",source_count-target_count)

#count_validation(source, target)

# dup_df = target.groupBy('empno').count().filter('count>1')
#
# dup_df.show()

def duplicate(dataframe, key_column):
    dup_df = target.groupBy(key_column).count().filter('count>1')
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
    else:
        print("No duplicates")

duplicate(target,"empno")

