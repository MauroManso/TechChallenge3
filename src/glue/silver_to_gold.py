"""
Glue Job: Silver to Gold
Cria agregações analíticas para o Tech Challenge.
Uses Spark SQL for reliable column handling.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
bucket = args['S3_BUCKET']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados Silver
silver_path = f"s3://{bucket}/silver/"
df = spark.read.parquet(silver_path)

print(f"Silver columns: {df.columns}")

gold_path = f"s3://{bucket}/gold/"

# Register temp view for SQL
df.createOrReplaceTempView("silver_data")

# =====================================================
# GOLD 1: Sintomas COVID por UF e Mês
# =====================================================
sintomas_uf_mes = spark.sql("""
SELECT 
    uf,
    uf_nome,
    regiao,
    v1013 as mes,
    COUNT(*) as total_entrevistados,
    SUM(CASE WHEN febre = 1 THEN 1 ELSE 0 END) as total_com_febre,
    SUM(CASE WHEN tosse = 1 THEN 1 ELSE 0 END) as total_com_tosse,
    SUM(CASE WHEN dif_respirar = 1 THEN 1 ELSE 0 END) as total_dificuldade_respirar,
    SUM(CASE WHEN perda_cheiro = 1 THEN 1 ELSE 0 END) as total_perda_cheiro,
    SUM(teve_sintomas_covid) as total_com_sintomas_covid,
    SUM(CASE WHEN procurou_atendimento = 1 THEN 1 ELSE 0 END) as total_procurou_atendimento,
    SUM(CASE WHEN internado = 1 THEN 1 ELSE 0 END) as total_internados,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    ROUND(SUM(teve_sintomas_covid) * 100.0 / COUNT(*), 2) as pct_sintomas_covid,
    ROUND(SUM(CASE WHEN procurou_atendimento = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_procurou_atendimento,
    ROUND(SUM(CASE WHEN internado = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_internados
FROM silver_data
GROUP BY uf, uf_nome, regiao, v1013
""")

sintomas_uf_mes.write.mode("overwrite").parquet(f"{gold_path}sintomas_uf_mes/")
print("Gold 1: sintomas_uf_mes written")

# =====================================================
# GOLD 2: Impacto no Trabalho por Região e Mês
# =====================================================
trabalho_regiao = spark.sql("""
SELECT 
    regiao,
    v1013 as mes,
    COUNT(*) as total_entrevistados,
    SUM(CASE WHEN afastou_trabalho = 1 THEN 1 ELSE 0 END) as total_afastados,
    SUM(CASE WHEN afastou_trabalho = 2 THEN 1 ELSE 0 END) as total_nao_afastados,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    ROUND(SUM(CASE WHEN afastou_trabalho = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_afastados
FROM silver_data
GROUP BY regiao, v1013
""")

trabalho_regiao.write.mode("overwrite").parquet(f"{gold_path}trabalho_regiao_mes/")
print("Gold 2: trabalho_regiao_mes written")

# =====================================================
# GOLD 3: Perfil Demográfico dos Sintomáticos
# =====================================================
perfil_sintomaticos = spark.sql("""
SELECT 
    sexo,
    cor_raca,
    COUNT(*) as total_sintomaticos,
    AVG(idade) as idade_media,
    SUM(CASE WHEN plano_saude = 1 THEN 1 ELSE 0 END) as com_plano_saude,
    SUM(CASE WHEN procurou_atendimento = 1 THEN 1 ELSE 0 END) as procurou_atendimento,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    CASE 
        WHEN sexo = 1 THEN 'Masculino'
        WHEN sexo = 2 THEN 'Feminino'
        ELSE 'Nao informado'
    END as sexo_desc,
    CASE 
        WHEN cor_raca = 1 THEN 'Branca'
        WHEN cor_raca = 2 THEN 'Preta'
        WHEN cor_raca = 3 THEN 'Amarela'
        WHEN cor_raca = 4 THEN 'Parda'
        WHEN cor_raca = 5 THEN 'Indigena'
        ELSE 'Nao informado'
    END as cor_raca_desc
FROM silver_data
WHERE teve_sintomas_covid = 1
GROUP BY sexo, cor_raca
""")

perfil_sintomaticos.write.mode("overwrite").parquet(f"{gold_path}perfil_sintomaticos/")
print("Gold 3: perfil_sintomaticos written")

# =====================================================
# GOLD 4: Testes COVID por UF
# =====================================================
testes_uf = spark.sql("""
SELECT 
    uf,
    uf_nome,
    regiao,
    COUNT(*) as total_entrevistados,
    SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) as total_testados,
    SUM(CASE WHEN resultado_teste = 1 THEN 1 ELSE 0 END) as total_positivos,
    SUM(CASE WHEN resultado_teste = 2 THEN 1 ELSE 0 END) as total_negativos,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    ROUND(SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_testados,
    CASE 
        WHEN SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) > 0 
        THEN ROUND(SUM(CASE WHEN resultado_teste = 1 THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END), 2)
        ELSE 0 
    END as taxa_positividade
FROM silver_data
WHERE fez_teste IS NOT NULL
GROUP BY uf, uf_nome, regiao
""")

testes_uf.write.mode("overwrite").parquet(f"{gold_path}testes_uf/")
print("Gold 4: testes_uf written")

# =====================================================
# GOLD 5: Evolução Temporal Nacional
# =====================================================
evolucao_nacional = spark.sql("""
SELECT 
    v1013 as mes,
    COUNT(*) as total_entrevistados,
    SUM(teve_sintomas_covid) as total_com_sintomas,
    SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) as total_testados,
    SUM(CASE WHEN resultado_teste = 1 THEN 1 ELSE 0 END) as total_positivos,
    SUM(CASE WHEN internado = 1 THEN 1 ELSE 0 END) as total_internados,
    SUM(CASE WHEN afastou_trabalho = 1 THEN 1 ELSE 0 END) as total_afastados_trabalho,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    ROUND(SUM(teve_sintomas_covid) * 100.0 / COUNT(*), 2) as pct_sintomaticos,
    ROUND(SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_testados
FROM silver_data
GROUP BY v1013
ORDER BY v1013
""")

evolucao_nacional.write.mode("overwrite").parquet(f"{gold_path}evolucao_nacional/")
print("Gold 5: evolucao_nacional written")

# =====================================================
# GOLD 6: Acesso a Saúde por Perfil Socioeconômico
# =====================================================
acesso_saude = spark.sql("""
SELECT 
    plano_saude,
    v1022 as situacao_domicilio,
    COUNT(*) as total_sintomaticos,
    SUM(CASE WHEN procurou_atendimento = 1 THEN 1 ELSE 0 END) as procurou_atendimento,
    SUM(CASE WHEN internado = 1 THEN 1 ELSE 0 END) as internados,
    SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) as fez_teste,
    AVG(idade) as idade_media,
    SUM(COALESCE(peso, 0)) as populacao_estimada,
    CASE 
        WHEN plano_saude = 1 THEN 'Com plano de saude'
        WHEN plano_saude = 2 THEN 'Sem plano de saude'
        ELSE 'Nao informado'
    END as plano_saude_desc,
    CASE 
        WHEN v1022 = 1 THEN 'Urbano'
        WHEN v1022 = 2 THEN 'Rural'
        ELSE 'Nao informado'
    END as situacao_desc,
    ROUND(SUM(CASE WHEN procurou_atendimento = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_procurou_atendimento,
    ROUND(SUM(CASE WHEN internado = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_internados,
    ROUND(SUM(CASE WHEN fez_teste = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_testados
FROM silver_data
WHERE teve_sintomas_covid = 1
GROUP BY plano_saude, v1022
""")

acesso_saude.write.mode("overwrite").parquet(f"{gold_path}acesso_saude/")
print("Gold 6: acesso_saude written")

print("All Gold tables written successfully!")
job.commit()