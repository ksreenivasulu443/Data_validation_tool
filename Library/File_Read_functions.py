from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Files_read").getOrCreate()

# df_csv = spark.read.option("header", True).option("delimiter",",").csv("path")
# df_json = spark.read.option("multiline", True).json("path")
# df_parquet = spark.read.parquet("path")
# df_avro = spark.read.avro("path")

import logging

logging.basicConfig(filename="newfile.log",
                    level=logging.INFO, #NDIWEC
                    filemode='w',
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()

def read_file(format,path,spark):
    if format.lower() == 'csv':
        source = spark.read.option("header", True).option("delimiter",",").csv(path)
        logger.info(" info CSV file has read successfully from the below path" + path)

    elif format.lower() == 'json':
        source = spark.read.json(path)
    elif format.lower() == 'parquet':
        source = spark.read.parquet(path)
    elif format.lower() == 'avro':
        source = spark.read.avro(path)
    else:
        logger.critical("File format is not found ")
    return source

def read_file1(format, path,spark):
    source = spark.read.format(format.lower()).load(path)












