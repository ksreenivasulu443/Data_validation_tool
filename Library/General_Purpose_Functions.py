from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()


def count_validation(sourceDF, targetDF,Out):
    source_count = sourceDF.count()
    target_count = targetDF.count()
    if source_count == target_count:
        print("Source count and target count is matching and count is", source_count)
        write_output(1,"Count_validation",source_count,target_count,"pass", source_count-target_count,Out)
    else:
        print("Source count and taget count is not matching and difference is",source_count-target_count)
        write_output(1, "Count_validation", source_count, target_count, "fail", source_count - target_count, Out)


def duplicate(dataframe, key_column,Out):
    dup_df = dataframe.groupBy(key_column).count().filter('count>1')
    target_count = dataframe.count()
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
        write_output(2, "duplicate", "NA", target_count, "Fail", dup_df.count(), Out)
    else:
        print("No duplicates")
        write_output(2, "duplicate", "NA", target_count, "pass", 0, Out)


def Uniquess_check(dataframe, unique_column,Out):
    target_count = dataframe.count()
    for column in unique_column:
        dup_df = dataframe.groupBy(column).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{column} columns has duplicate")
            dup_df.show(10)
            write_output(3, "Uniqueness", "NA", target_count, "Fail", dup_df.count(), Out)
        else:
            print("All records has unique records")
            write_output(3, "Uniqueness", "NA", target_count, "Pass", 0, Out)

def Null_value_check(dataframe, Null_columns,Out):
    target_count = dataframe.count()
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
            write_output(4, "Null_value_check", "NA", target_count, "fail", cnt[0][0], Out)


        else:
            print("No null records present")
            write_output(4, "Null_value_check", "NA", target_count, "pass", 0, Out)




def records_present_only_in_target(source,target,keyList,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("SourceCount is null").count()
    print("Key column record present in target but not in Source :" + str(count))
    if count > 0:
        count_compare.filter("SourceCount is null").show()
    else:
        print("No extra records present in source")

def records_present_only_in_source(source,target,keyList,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("TargetCount is null").count()
    print("Key column record present in Source but not in target :" + str(count))
    if count > 0:
        count_compare.filter("TargetCount is null").show()
    else:
        print("No extra records present")

def data_compare( source, target,keycolumn,Out):
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


def write_output(TC_ID,Test_Case_Name,Number_of_source_Records,Number_of_target_Records,Status,Number_of_failed_Records,Out):
    Out["TC_ID"].append(TC_ID)
    Out["test_Case_Name"].append(Test_Case_Name)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)
















