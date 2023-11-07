from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()


def count_validation(sourceDF, targetDF):
    source_count = sourceDF.count()
    target_count = targetDF.count()
    if source_count == target_count:
        print("Source count and target count is matching and count is", source_count)
    else:
        print("Source count and taget count is not matching and difference is",source_count-target_count)

def duplicate(dataframe, key_column):
    dup_df = dataframe.groupBy(key_column).count().filter('count>1')
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
    else:
        print("No duplicates")


def Uniquess_check(dataframe, unique_column):
    for column in unique_column:
        dup_df = dataframe.groupBy(column).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{column} columns has duplicate")
            dup_df.show(10)
        else:
            print("All records has unique records")

def Null_value(dataframe, Null_columns):
    for column in Null_columns:
        Null_df = dataframe.select(count(when(col(column).contains('None') | \
                                        col(column).contains('NULL') | \
                                        col(column).contains('Null') | \
                                        (col(column) == '') | \
                                        col(column).isNull() | \
                                        isnan(column), column
                                        )).alias("Null_value_count"))
        cnt = Null_df.collect()
        if cnt[0][0]>=1:
            print(f"{column} columns has Null values")
            Null_df.show(10)
        else:
                print("No null records present")


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



















