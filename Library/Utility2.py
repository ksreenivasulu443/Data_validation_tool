from pyspark.sql import SparkSession
from Library.File_Read_functions import read_file
from Library.Database_Read_Functions import db_read
from pyspark.sql.functions import count, when, isnan, isnull, col, trim

spark = SparkSession.builder.master("local").appName("Data val func").getOrCreate()

# Count validation

source = read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee.csv", spark=spark)
#source.show()
#print("#"*40)
# target = db_read(url="jdbc:oracle:thin:@//localhost:1521/freepdb1",username='scott',password='tiger',query="""select * from emp """,driver='oracle.jdbc.driver.OracleDriver', spark=spark)
# target.show()
# #target= read_file(format="csv",path="/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/employee_t.csv", spark=spark)
#
source.createOrReplaceTempView("source")
spark.sql("select empno, ename, job, mgr,sal*200 sal, nvl(comm,0) comm, deptno from source").show()

spark.sql("select empno,count(1) source_cnt from source group by empno having count(1)>1").show()


spark.sql("select empno, comm source_cnt from source where comm is null").show()




#spark.read.format('avro').load("/Users/harish/Downloads/Sample-Avro-File-Format.avro").show()