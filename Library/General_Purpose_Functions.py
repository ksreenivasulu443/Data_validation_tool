from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()

# Count validation

source = read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee.csv", spark=spark)
#source.show()
#print("#"*40)
target = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp1 """,driver='oracle.jdbc.driver.OracleDriver')
target.show()


def count_validation(sourceDF, targetDF):
    source_count = sourceDF.count()
    target_count = targetDF.count()
    if source_count == target_count:
        print("Source count and target count is matching and count is", source_count)
    else:
        print("Source count and taget count is not matching and difference is",source_count-target_count)


def duplicate(dataframe, key_column):
    dup_df = target.groupBy(key_column).count().filter('count>1')
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
    else:
        print("No duplicates")

def Uniquess_check(dataframe, unique_column):
    for i in unique_column:
        dup_df = target.groupBy(i).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{i} columns has duplicate")
            dup_df.show(10)
        else:
            print("All records has unique records")
#Uniquess_check(target,['EMPNO', 'ENAME'])

def Null_value(dataframe, Null_columns):
    for c in Null_columns:

        Null_df = dataframe.select(count(when(col(c).contains('None') | \
                                        col(c).contains('NULL') | \
                                        (col(c) == '') | \
                                        col(c).isNull() | \
                                        isnan(c), c
                                        )).alias("Null_value_count"))
        if Null_df.count()>0:
            print(f"{c} columns has Null values")
            Null_df.show(10)
        else:
                print("All records has no null records")

source.show()
#Null_value(source,['bonus','Empno'])

def data_compare( source, target,keycolumn):
    for colname in source.columns:
        source = source.withColumn(colname, trim(col(colname)))

data_compare(source, target,keycolumn='empno')

def records_present_only_in_target(source,target,keyList):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("SourceCount is null").count()
    print("Key column record present in target but not in Source :" + str(count))
    if count > 0:
        count_compare.filter("SourceCount is null").show()
    else:
        print("No extra records present in source")

def records_present_only_in_source(source,target,keyList):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("TargetCount is null").count()
    print("Key column record present in Source but not in target :" + str(count))
    if count > 0:
        count_compare.filter("TargetCount is null").show()
    else:
        print("No extra records present")














