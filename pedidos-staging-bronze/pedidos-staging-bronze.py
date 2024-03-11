import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import *
from pyspark.sql.functions import when, col, lower, regexp_replace, count
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DecimalType, LongType, DateType
from pyspark.sql.functions import col, current_timestamp, lit
from uuid import uuid4

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialize o contexto do Spark e o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated from Amazon S3
df = spark.read.options(delimiter=";", header=True).csv("s3://pedidos-bronze/staging")

output_path = "s3://pedidos-bronze/bronze"
df.write.mode("overwrite").parquet(output_path)


