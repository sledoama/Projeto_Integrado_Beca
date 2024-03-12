import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import *
from uuid import uuid4
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, lower, regexp_replace, year, month, datediff, sum, to_date, countDistinct, avg, trim, current_timestamp, input_file_name, lit
)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialize o contexto do Spark e o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("id_cliente", StringType(), True),
    StructField("data_pedido", TimestampType(), True),
    StructField("ped_id", StringType(), True),
    StructField("id_produto", StringType(), True),
    StructField("qtd", IntegerType(), True),
    StructField("id_canal", StringType(), True),
    StructField("forma_de_pagamento", StringType(), True),
    StructField("data_entrega", TimestampType(), True),
    StructField("nota_fiscal", StringType(), True)
])

# Lê o arquivo CSV com o schema definido
df_silver = spark.read.options(delimiter=";", header=True).schema(schema).csv("s3://pedidos-bronze/staging")

# Renomeia colunas e também trata a coluna 'forma_de_pagamento'
df_silver = (df_silver
             .withColumnRenamed("ped_id", "id_pedido")
             .withColumnRenamed("qtd", "quantidade_produto")
             .withColumn("forma_de_pagamento", lower(col("forma_de_pagamento")))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "xeque", "cheque"))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "cr[eéèëẽ]+dito", "crédito"))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "d[eéèëẽ]+bito", "débito"))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "pics", "pix"))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "déebito", "débito"))
             .withColumn("forma_de_pagamento", regexp_replace("forma_de_pagamento", "crẽditodèbito", "crédito-débito"))
             .withColumn("ano", year("data_pedido"))
             .withColumn("mes", month("data_pedido"))
             .withColumn("data_carga", lit(datetime.now().date()))
             .withColumn("batch_id", lit(str(uuid4())))
             .withColumn("input_file_name", input_file_name())
             .withColumn("data_source", lit("pedidos-bronze"))
             .withColumn("periodo_entrega", datediff(col("data_entrega"), col("data_pedido")))
            )

df_silver = df_silver.select(
    "id_cliente",
    "data_pedido",
    "ano",
    "mes",
    "id_pedido",
    "id_produto",
    "quantidade_produto",
    "id_canal",
    "forma_de_pagamento",
    "data_entrega",
    "periodo_entrega",
    "nota_fiscal",
    "data_carga",
    "batch_id",
    "input_file_name",
    "data_source"
)

# # Envie o arquivo para o S3
database = "pedidos-silver"
tabela = "pedidos_loja"

# Variáveis
s3_bucket_path = "s3://pedidos-silver/pedidos_loja/"
mode = "overwrite"
database_table_name = f"`{database}`.`{tabela}`"

# Salva o DataFrame como uma nova tabela no catálogo de dados do Glue, especificando o caminho no S3
df_silver.write.mode(mode).option("path", s3_bucket_path).saveAsTable(database_table_name)
