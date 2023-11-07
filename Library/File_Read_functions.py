
import logging

logging.basicConfig(filename="newfile.log",
                    level=logging.INFO, #NDIWEC
                    filemode='w',
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()

def read_file(format,path,spark):
    if format.lower() == 'csv':
        source = spark.read.option("header", True).option("delimiter",",").csv(path)
        logger.info("CSV file has read successfully from the below path" + path)

    elif format.lower() == 'json':
        source = spark.read.json(path)
        logger.info("Json file has read successfully from the below path" + path)

    elif format.lower() == 'parquet':
        source = spark.read.parquet(path)
        logger.info("parquet file has read successfully from the below path" + path)

    elif format.lower() == 'avro':
        source = spark.read.avro(path)
        logger.info("Avro file has read successfully from the below path" + path)
    else:
        logger.critical("File format is not found ")
    return source

def read_file1(format, path,spark):
    source = spark.read.format(format.lower()).load(path)












