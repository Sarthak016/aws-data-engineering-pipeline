import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper, when

## Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Read raw data from S3
## recursiveFileLookup=true avoids partition column conflict
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("recursiveFileLookup", "true") \
    .csv("s3://sarthak-de-raw-data/orders/")

## Transformations
# Add order category based on amount
df = df.withColumn("order_category",
    when(col("amount") >= 20000, "High Value")
    .when(col("amount") >= 5000, "Mid Value")
    .otherwise("Low Value")
)

# Uppercase customer names
df = df.withColumn("customer_name", upper(col("customer_name")))

# Filter null order IDs
df = df.filter(col("order_id").isNotNull())

## Write cleaned data back to S3 as Parquet
df.write.mode("overwrite").parquet("s3://sarthak-de-raw-data/processed/orders/")

job.commit()
