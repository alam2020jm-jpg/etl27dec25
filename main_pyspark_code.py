from pyspark.sql import SparkSession
import sys
spark=SparkSession\
     .builder.appName("Myetl27dec25")\
     .config("spark.sql.dynamicAllaocation.autoBroadcastJoinThreshold","50MB")\
     .getOrCreate()
input_data_path=sys.argv[1]
gcs_Out_path=sys.argv[2]
bq_out_table=sys.argv[3]
df=spark.read.format("csv")\
  .option('mode','PERMISSIVE')\
  .option("columnNameofCorruptRecord","CorrutedRecords")\
  .option("header",True)\
  .option("inferSchema",True)\
  .load(input_data_path)
df.printSchema()
df.write.format("csv")\
  .option('mode',"append")\
  .option("header",True)\
  .save(gcs_Out_path)
df.write.format("bigquery")\
  .option('table',bq_out_table)\
  .option("TemporaryGCSBucket","gs://tembqpath")\
  .option('mode',"append")\
  .save()
spark.stop()
