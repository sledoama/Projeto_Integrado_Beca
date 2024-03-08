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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialize o contexto do Spark e o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated from Amazon S3
AmazonS3 = spark.read.options(delimiter=";", header=True).csv("s3://pedidos-bronze/loja")

# Tratamento dos tipos de dados
AmazonS3 = AmazonS3.withColumn("id_cliente", col("id_cliente").cast(StringType())) \
       .withColumn("data_pedido", col("data_pedido").cast(TimestampType())) \
       .withColumn("ped_id", col("ped_id").cast(StringType())) \
       .withColumn("id_produto", col("id_produto").cast(StringType())) \
       .withColumn("qtd", col("qtd").cast(IntegerType())) \
       .withColumn("id_canal", col("id_canal").cast(StringType())) \
       .withColumn("forma_de_pagamento", col("forma_de_pagamento").cast(StringType())) \
       .withColumn("data_entrega", col("data_entrega").cast(TimestampType())) \
       .withColumn("nota_fiscal", col("nota_fiscal").cast(StringType()))

AmazonS3.limit(5).show()

#Tratamento da coluna "forma_de_pagamento" 
AmazonS3 = AmazonS3.withColumn("forma_de_pagamento", lower(AmazonS3["forma_de_pagamento"])) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "xeque", "cheque")) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "cr[eéèëẽ]+dito", "crédito")) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "d[eéèëẽ]+bito", "débito")) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "pics", "pix")) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "déebito", "débito")) \
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "crẽditodèbito", "créditodébito"))


# Envie o arquivo para o S3
#output_path = "s3://pedidos-silver/pedidos_loja/"
#AmazonS3.write.mode("overwrite").parquet(output_path)
database = "pedidos-silver"
tabela = "pedidos_loja"
output_path = "s3://pedidos-silver/loja/"
AmazonS3.write.insertInto(f"`{database}`.`{tabela}`", overwrite=True)