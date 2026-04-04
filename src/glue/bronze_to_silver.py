"""
Glue Job: Bronze to Silver
Transforma dados brutos CSV em Parquet com limpeza e tipagem.
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
df = spark.read.option("header", "true").option("delimiter", ";").csv(bronze_path)

# Mapeamento de colunas principais para nomes descritivos
column_mapping = {
    "Ano": "ano",
    "UF": "uf_codigo",
    "V1012": "semana_mes",
    "V1013": "mes_pesquisa",
    "V1022": "situacao_domicilio",  # 1=Urbana, 2=Rural
    "V1023": "tipo_area",
    "A002": "idade",
    "A003": "sexo",  # 1=Homem, 2=Mulher
    "A004": "cor_raca",  # 1=Branca, 2=Preta, 3=Amarela, 4=Parda, 5=Indígena
    "A005": "escolaridade",
    "B0011": "teve_febre",
    "B0012": "teve_tosse",
    "B0013": "teve_dor_garganta",
    "B0014": "teve_dificuldade_respirar",
    "B0015": "teve_dor_cabeca",
    "B0016": "teve_dor_peito",
    "B0017": "teve_nausea",
    "B0018": "teve_nariz_entupido",
    "B0019": "teve_fadiga",
    "B00110": "teve_dor_olhos",
    "B00111": "teve_perda_cheiro",
    "B00112": "teve_dor_muscular",
    "B00113": "teve_diarreia",
    "B002": "procurou_atendimento",
    "B006": "ficou_internado",
    "B007": "plano_saude",  # 1=Sim, 2=Não
    "B009B": "fez_teste_covid",
    "B011": "resultado_teste",  # 1=Positivo, 2=Negativo, 3=Inconclusivo
    "C001": "afastou_trabalho",
    "C002": "motivo_afastamento",
    "C007": "tipo_trabalho",
    "C010": "atividade_principal",
    "V1027": "peso_pos_estratificacao",
    "V1028": "peso_primario",
    "V1029": "projecao_populacao"
}

# Aplicar renomeação
for old_name, new_name in column_mapping.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# Converter colunas numéricas
numeric_columns = [
    "ano", "uf_codigo", "semana_mes", "mes_pesquisa", "idade",
    "situacao_domicilio", "sexo", "cor_raca", "escolaridade",
    "teve_febre", "teve_tosse", "teve_dor_garganta", "teve_dificuldade_respirar",
    "procurou_atendimento", "ficou_internado", "plano_saude",
    "fez_teste_covid", "resultado_teste", "afastou_trabalho"
]

for col in numeric_columns:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(IntegerType()))

# Converter colunas de peso para double
weight_columns = ["peso_pos_estratificacao", "peso_primario", "projecao_populacao"]
for col in weight_columns:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))

# Adicionar colunas derivadas
df = df.withColumn(
    "uf_nome",
    F.when(F.col("uf_codigo") == 11, "Rondônia")
    .when(F.col("uf_codigo") == 12, "Acre")
    .when(F.col("uf_codigo") == 13, "Amazonas")
    .when(F.col("uf_codigo") == 14, "Roraima")
    .when(F.col("uf_codigo") == 15, "Pará")
    .when(F.col("uf_codigo") == 16, "Amapá")
    .when(F.col("uf_codigo") == 17, "Tocantins")
    .when(F.col("uf_codigo") == 21, "Maranhão")
    .when(F.col("uf_codigo") == 22, "Piauí")
    .when(F.col("uf_codigo") == 23, "Ceará")
    .when(F.col("uf_codigo") == 24, "Rio Grande do Norte")
    .when(F.col("uf_codigo") == 25, "Paraíba")
    .when(F.col("uf_codigo") == 26, "Pernambuco")
    .when(F.col("uf_codigo") == 27, "Alagoas")
    .when(F.col("uf_codigo") == 28, "Sergipe")
    .when(F.col("uf_codigo") == 29, "Bahia")
    .when(F.col("uf_codigo") == 31, "Minas Gerais")
    .when(F.col("uf_codigo") == 32, "Espírito Santo")
    .when(F.col("uf_codigo") == 33, "Rio de Janeiro")
    .when(F.col("uf_codigo") == 35, "São Paulo")
    .when(F.col("uf_codigo") == 41, "Paraná")
    .when(F.col("uf_codigo") == 42, "Santa Catarina")
    .when(F.col("uf_codigo") == 43, "Rio Grande do Sul")
    .when(F.col("uf_codigo") == 50, "Mato Grosso do Sul")
    .when(F.col("uf_codigo") == 51, "Mato Grosso")
    .when(F.col("uf_codigo") == 52, "Goiás")
    .when(F.col("uf_codigo") == 53, "Distrito Federal")
    .otherwise("Desconhecido")
)

df = df.withColumn(
    "regiao",
    F.when(F.col("uf_codigo").between(11, 17), "Norte")
    .when(F.col("uf_codigo").between(21, 29), "Nordeste")
    .when(F.col("uf_codigo").between(31, 35), "Sudeste")
    .when(F.col("uf_codigo").between(41, 43), "Sul")
    .when(F.col("uf_codigo").between(50, 53), "Centro-Oeste")
    .otherwise("Desconhecido")
)

# Indicador de sintomas COVID
df = df.withColumn(
    "teve_sintomas_covid",
    F.when(
        (F.col("teve_febre") == 1) |
        (F.col("teve_tosse") == 1) |
        (F.col("teve_dificuldade_respirar") == 1) |
        (F.col("teve_perda_cheiro") == 1),
        1
    ).otherwise(0)
)

# Remover duplicatas
df = df.dropDuplicates()

# Repartir por mês e escrever em Parquet
silver_path = f"s3://{bucket}/silver/"
df.write.mode("overwrite").partitionBy("ano", "mes_pesquisa").parquet(silver_path)

job.commit()