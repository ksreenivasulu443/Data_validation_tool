from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Files_read").getOrCreate()

# df_csv = spark.read.option("header", True).option("delimiter",",").csv("path")
# df_json = spark.read.option("multiline", True).json("path")
# df_parquet = spark.read.parquet("path")
# df_avro = spark.read.avro("path")

def read_file(format,path):
    if format.lower() == 'csv':
        source = spark.read.option("header", True).option("delimiter",",").csv(path)
    elif format.lower() == 'json':
        source = spark.read.json(path)
    elif format.lower() == 'parquet':
        source = spark.read.parquet(path)
    elif format.lower() == 'avro':
        source = spark.read.avro(path)
    return source

def read_file1(format, path):
    source = spark.read.format(format.lower()).load(path)

source = read_file("csv", path ='/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/IPL Matches 2008-2020.csv')
source.show()


source_json = read_file("json", path ='/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/singleline.json')
source_json.show()










