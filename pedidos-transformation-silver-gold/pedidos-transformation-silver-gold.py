import logging
import sys
from datetime import datetime
from uuid import uuid4
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct, lower, regexp_replace, sum, to_date, trim, year, month, datediff
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialização do contexto do Spark e do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Leitura do arquivo Parquet
df_gold = spark.read.parquet("s3://pedidos-silver/pedidos_loja/")

# Leitura dos dados do produto
produtos = spark.read.option("delimiter", "|").option("header", "true").csv("s3://athosbucketimage/mock_data/produto_dataset_moc.csv")

# Remoção de colunas desnecessárias
columns_to_drop = ["data_source", "input_file_name", "batch_id", "data_carga", "nota_fiscal"]
df_gold = df_gold.drop(*columns_to_drop)
df_gold = df_gold.withColumn("data_pedido", to_date(col("data_pedido")))

# Renomeação e conversão de tipos de colunas em produtos
produtos = produtos.withColumnRenamed("IdProduto", "id_produto") \
                   .withColumn("id_produto", col("id_produto").cast(StringType())) \
                   .withColumn("TipoProduto", col("TipoProduto").cast(StringType()))

# Join entre df_gold e produtos usando 'id_produto' como chave
joined_df = df_gold.join(produtos, on="id_produto", how="left")

columns_to_drop = ["saldo", "frete", "batch_id"]
joined_df = joined_df.drop(*columns_to_drop)

# Salvar o resultado final no S3 e registrar no catálogo de dados do Glue
database = "pedidos-gold"
tabela = "pedidos_loja"
joined_df.write.mode("overwrite").option("path", f"s3://pedidos-gold/{tabela}").saveAsTable(f"`{database}`.`{tabela}`")