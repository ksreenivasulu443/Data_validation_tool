


def db_read(url,username, password,query,driver,spark):
    df= spark.read.format("jdbc"). \
        option("url", url). \
        option("password", password). \
        option("user", username). \
        option("query", query). \
        option("driver", driver).load()
    return df

def db_write(df,url,username, password,table,driver,spark):
    df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table) \
        .option("user", username) \
        .option("password", password) \
        .save()

def kafka_read(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
        .option("subscribe", "topic1") \
        .option("includeHeaders", "true") \
        .load()
    return df




















#
# source_oracle = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp where ename='KING'""",driver='oracle.jdbc.driver.OracleDriver')
# source_oracle.show()
#
#
# source_ps = db_read(url = "jdbc:postgresql://localhost:5432/postgres", username="postgres", password='Dharmavaram1@', query ="""select * from cars""", driver='org.postgresql.Driver')
# source_ps.show()



