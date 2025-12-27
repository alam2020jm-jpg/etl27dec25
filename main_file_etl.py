import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Arguments from Cloud Function
INPUT_GCS_PATH = sys.argv[1]
OUTPUT_FOLDER = sys.argv[2]

spark = SparkSession.builder \
    .appName("ETL_Bonus_Transformation") \
    .config("temporaryGcsBucket", "dec251225") \
    .getOrCreate()
# STEP 1: Raw Data Read [cite: 288-293]
df = spark.read.csv(INPUT_GCS_PATH, header=True, inferSchema=True)

# STEP 2: Transformation - Add Bonus & Total Salary [cite: 206, 307-316]
# Fixed Rs 5000 bonus column add kar rahe hain
df_with_bonus = df.withColumn("bonus", lit(5000))

# Salary + Bonus ka total calculate kar rahe hain
df_final = df_with_bonus.withColumn("total_credited_salary", col("salary") + col("bonus"))

# STEP 3: Write to Processed Bucket [cite: 347-354]p
output_path = f"{OUTPUT_FOLDER}/processdataetl/final_output/"
df_final.write.mode("overwrite").csv(output_path)
print(f"✅ ETL Complete! Transformed data saved to: {output_path}")
#STEP 4: Write to Processed Bucket [cite: 347-354]p
table_path="banglore251225.ban251225.proceddedtable"
df_final_bq=df_final.write.format("bigquery")\
            .mode("append")\
            .option("table",table_path)\
            .option("parentProject", "banglore251225") \
            .save()
print(f"✅ ETL Complete! Transformed data saved to: {table_path}")
spark.stop()