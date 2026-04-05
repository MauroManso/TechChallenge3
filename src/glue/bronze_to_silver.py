"""
Glue Job: Bronze to Silver
Transforma dados brutos CSV em Parquet com limpeza e tipagem.
Uses Spark SQL for more reliable column handling.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Parâmetros do Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
bucket = args['S3_BUCKET']

# Inicialização
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados Bronze
bronze_path = f"s3://{bucket}/bronze/"
df = spark.read.option("header", "true").option("delimiter", ",").csv(bronze_path)

# Log original columns for debugging
print(f"Original columns count: {len(df.columns)}")
print(f"First 10 columns: {df.columns[:10]}")

# Normalize all column names to lowercase using toDF
lower_cols = [c.lower().strip() for c in df.columns]
df = df.toDF(*lower_cols)

print(f"Normalized columns: {df.columns[:10]}")

# Register as temp view for SQL operations
df.createOrReplaceTempView("bronze_data")

# Use Spark SQL for transformations - more reliable than DataFrame API for column names
df = spark.sql("""
SELECT 
    *,
    CASE 
        WHEN uf = 11 THEN 'Rondonia'
        WHEN uf = 12 THEN 'Acre'
        WHEN uf = 13 THEN 'Amazonas'
        WHEN uf = 14 THEN 'Roraima'
        WHEN uf = 15 THEN 'Para'
        WHEN uf = 16 THEN 'Amapa'
        WHEN uf = 17 THEN 'Tocantins'
        WHEN uf = 21 THEN 'Maranhao'
        WHEN uf = 22 THEN 'Piaui'
        WHEN uf = 23 THEN 'Ceara'
        WHEN uf = 24 THEN 'Rio Grande do Norte'
        WHEN uf = 25 THEN 'Paraiba'
        WHEN uf = 26 THEN 'Pernambuco'
        WHEN uf = 27 THEN 'Alagoas'
        WHEN uf = 28 THEN 'Sergipe'
        WHEN uf = 29 THEN 'Bahia'
        WHEN uf = 31 THEN 'Minas Gerais'
        WHEN uf = 32 THEN 'Espirito Santo'
        WHEN uf = 33 THEN 'Rio de Janeiro'
        WHEN uf = 35 THEN 'Sao Paulo'
        WHEN uf = 41 THEN 'Parana'
        WHEN uf = 42 THEN 'Santa Catarina'
        WHEN uf = 43 THEN 'Rio Grande do Sul'
        WHEN uf = 50 THEN 'Mato Grosso do Sul'
        WHEN uf = 51 THEN 'Mato Grosso'
        WHEN uf = 52 THEN 'Goias'
        WHEN uf = 53 THEN 'Distrito Federal'
        ELSE 'Desconhecido'
    END AS uf_nome,
    CASE 
        WHEN uf BETWEEN 11 AND 17 THEN 'Norte'
        WHEN uf BETWEEN 21 AND 29 THEN 'Nordeste'
        WHEN uf BETWEEN 31 AND 35 THEN 'Sudeste'
        WHEN uf BETWEEN 41 AND 43 THEN 'Sul'
        WHEN uf BETWEEN 50 AND 53 THEN 'Centro-Oeste'
        ELSE 'Desconhecido'
    END AS regiao,
    CASE 
        WHEN b0011 = 1 OR b0012 = 1 OR b0014 = 1 OR b00111 = 1 THEN 1
        ELSE 0
    END AS teve_sintomas_covid,
    CAST(ano AS INT) AS ano_int,
    CAST(uf AS INT) AS uf_int,
    CAST(v1013 AS INT) AS mes_int,
    CAST(a002 AS INT) AS idade,
    CAST(a003 AS INT) AS sexo,
    CAST(a004 AS INT) AS cor_raca,
    CAST(b0011 AS INT) AS febre,
    CAST(b0012 AS INT) AS tosse,
    CAST(b0014 AS INT) AS dif_respirar,
    CAST(b00111 AS INT) AS perda_cheiro,
    CAST(b002 AS INT) AS procurou_atendimento,
    CAST(b006 AS INT) AS internado,
    CAST(b007 AS INT) AS plano_saude,
    CAST(b009b AS INT) AS fez_teste,
    CAST(b011 AS INT) AS resultado_teste,
    CAST(c001 AS INT) AS afastou_trabalho,
    CAST(v1032 AS DOUBLE) AS peso
FROM bronze_data
""")

# Remove duplicates
df = df.dropDuplicates()

# Write to Silver as Parquet partitioned by ano and mes
silver_path = f"s3://{bucket}/silver/"
df.write.mode("overwrite").partitionBy("ano", "v1013").parquet(silver_path)

print(f"Silver data written to {silver_path}")

job.commit()