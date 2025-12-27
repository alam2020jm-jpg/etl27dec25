from pyspar.sql import SparkSession
import sys
spark=SparkSession\
     .builder.appName("Myetl27dec25")\
     .config("spark.sql.dynamicAllaocation.autoBroadcastJoinThreshold","50MB")\
     .getOrCreate()
input_data_path=sys.argv[0]
gcs-Out_path=sys.argv[1]
bq_out_table=sys.argv[2]
df=spark.read.format("csv")\
  .option('mode','PERMISSIVE')\
  .option("columnNameofCorruptRecord","CorrutedRecords")\
  .option("header",True)\
  .option("inferSchema",True)\
  .load(input_data_path)
df.printSchema()
 
  .
