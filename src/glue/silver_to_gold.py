"""
Glue Job: Silver to Gold
Cria agregações analíticas para o Tech Challenge.
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

gold_path = f"s3://{bucket}/gold/"

# =====================================================
# GOLD 1: Sintomas COVID por UF e Mês
# =====================================================
sintomas_uf_mes = df.groupBy("uf_codigo", "uf_nome", "regiao", "mes_pesquisa").agg(
    F.count("*").alias("total_entrevistados"),
    F.sum("teve_febre").alias("total_com_febre"),
    F.sum("teve_tosse").alias("total_com_tosse"),
    F.sum("teve_dificuldade_respirar").alias("total_dificuldade_respirar"),
    F.sum("teve_perda_cheiro").alias("total_perda_cheiro"),
    F.sum("teve_sintomas_covid").alias("total_com_sintomas_covid"),
    F.sum(F.when(F.col("procurou_atendimento") == 1, 1).otherwise(0)).alias("total_procurou_atendimento"),
    F.sum(F.when(F.col("ficou_internado") == 1, 1).otherwise(0)).alias("total_internados"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

# Calcular percentuais
sintomas_uf_mes = sintomas_uf_mes.withColumn(
    "pct_sintomas_covid",
    F.round((F.col("total_com_sintomas_covid") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "pct_procurou_atendimento",
    F.round((F.col("total_procurou_atendimento") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "pct_internados",
    F.round((F.col("total_internados") / F.col("total_entrevistados")) * 100, 2)
)

sintomas_uf_mes.write.mode("overwrite").parquet(f"{gold_path}sintomas_uf_mes/")

# =====================================================
# GOLD 2: Impacto no Trabalho por Região e Mês
# =====================================================
trabalho_regiao = df.groupBy("regiao", "mes_pesquisa").agg(
    F.count("*").alias("total_entrevistados"),
    F.sum(F.when(F.col("afastou_trabalho") == 1, 1).otherwise(0)).alias("total_afastados"),
    F.sum(F.when(F.col("afastou_trabalho") == 2, 1).otherwise(0)).alias("total_nao_afastados"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

trabalho_regiao = trabalho_regiao.withColumn(
    "pct_afastados",
    F.round((F.col("total_afastados") / F.col("total_entrevistados")) * 100, 2)
)

trabalho_regiao.write.mode("overwrite").parquet(f"{gold_path}trabalho_regiao_mes/")

# =====================================================
# GOLD 3: Perfil Demográfico dos Sintomáticos
# =====================================================
perfil_sintomaticos = df.filter(F.col("teve_sintomas_covid") == 1).groupBy(
    "sexo", "cor_raca", "escolaridade"
).agg(
    F.count("*").alias("total_sintomaticos"),
    F.avg("idade").alias("idade_media"),
    F.sum(F.when(F.col("plano_saude") == 1, 1).otherwise(0)).alias("com_plano_saude"),
    F.sum(F.when(F.col("procurou_atendimento") == 1, 1).otherwise(0)).alias("procurou_atendimento"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

# Adicionar descrições
perfil_sintomaticos = perfil_sintomaticos.withColumn(
    "sexo_desc",
    F.when(F.col("sexo") == 1, "Masculino")
    .when(F.col("sexo") == 2, "Feminino")
    .otherwise("Não informado")
).withColumn(
    "cor_raca_desc",
    F.when(F.col("cor_raca") == 1, "Branca")
    .when(F.col("cor_raca") == 2, "Preta")
    .when(F.col("cor_raca") == 3, "Amarela")
    .when(F.col("cor_raca") == 4, "Parda")
    .when(F.col("cor_raca") == 5, "Indígena")
    .otherwise("Não informado")
)

perfil_sintomaticos.write.mode("overwrite").parquet(f"{gold_path}perfil_sintomaticos/")

# =====================================================
# GOLD 4: Testes COVID por UF
# =====================================================
testes_uf = df.filter(F.col("fez_teste_covid").isNotNull()).groupBy("uf_codigo", "uf_nome", "regiao").agg(
    F.count("*").alias("total_entrevistados"),
    F.sum(F.when(F.col("fez_teste_covid") == 1, 1).otherwise(0)).alias("total_testados"),
    F.sum(F.when(F.col("resultado_teste") == 1, 1).otherwise(0)).alias("total_positivos"),
    F.sum(F.when(F.col("resultado_teste") == 2, 1).otherwise(0)).alias("total_negativos"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

testes_uf = testes_uf.withColumn(
    "pct_testados",
    F.round((F.col("total_testados") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "taxa_positividade",
    F.when(
        F.col("total_testados") > 0,
        F.round((F.col("total_positivos") / F.col("total_testados")) * 100, 2)
    ).otherwise(0)
)

testes_uf.write.mode("overwrite").parquet(f"{gold_path}testes_uf/")

# =====================================================
# GOLD 5: Evolução Temporal Nacional
# =====================================================
evolucao_nacional = df.groupBy("mes_pesquisa").agg(
    F.count("*").alias("total_entrevistados"),
    F.sum("teve_sintomas_covid").alias("total_com_sintomas"),
    F.sum(F.when(F.col("fez_teste_covid") == 1, 1).otherwise(0)).alias("total_testados"),
    F.sum(F.when(F.col("resultado_teste") == 1, 1).otherwise(0)).alias("total_positivos"),
    F.sum(F.when(F.col("ficou_internado") == 1, 1).otherwise(0)).alias("total_internados"),
    F.sum(F.when(F.col("afastou_trabalho") == 1, 1).otherwise(0)).alias("total_afastados_trabalho"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
).orderBy("mes_pesquisa")

evolucao_nacional = evolucao_nacional.withColumn(
    "pct_sintomaticos",
    F.round((F.col("total_com_sintomas") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "pct_testados",
    F.round((F.col("total_testados") / F.col("total_entrevistados")) * 100, 2)
)

evolucao_nacional.write.mode("overwrite").parquet(f"{gold_path}evolucao_nacional/")

# =====================================================
# GOLD 6: Indicadores Econômicos por Região
# =====================================================
# Análise de impacto econômico mais abrangente
economico_regiao = df.groupBy("regiao", "mes_pesquisa").agg(
    F.count("*").alias("total_entrevistados"),
    # Trabalho
    F.sum(F.when(F.col("afastou_trabalho") == 1, 1).otherwise(0)).alias("total_afastados"),
    F.sum(F.when(F.col("C011A") == 1, 1).otherwise(0)).alias("trabalho_remoto"),
    # Tipo de ocupação
    F.sum(F.when(F.col("tipo_trabalho") == 1, 1).otherwise(0)).alias("empregados_formais"),
    F.sum(F.when(F.col("tipo_trabalho") == 2, 1).otherwise(0)).alias("empregados_informais"),
    F.sum(F.when(F.col("tipo_trabalho") == 3, 1).otherwise(0)).alias("conta_propria"),
    # Auxílio emergencial
    F.sum(F.when(F.col("F001") == 1, 1).otherwise(0)).alias("recebeu_auxilio"),
    # Renda (média dos que informaram)
    F.avg(F.when(F.col("C013").isNotNull() & (F.col("C013") > 0), F.col("C013"))).alias("renda_media_habitual"),
    F.avg(F.when(F.col("C014").isNotNull() & (F.col("C014") > 0), F.col("C014"))).alias("renda_media_efetiva"),
    # Pesos
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

# Calcular percentuais e indicadores derivados
economico_regiao = economico_regiao.withColumn(
    "pct_afastados",
    F.round((F.col("total_afastados") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "pct_trabalho_remoto",
    F.round((F.col("trabalho_remoto") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "pct_informal",
    F.round((F.col("empregados_informais") / 
            (F.col("empregados_formais") + F.col("empregados_informais") + F.col("conta_propria") + 1)) * 100, 2)
).withColumn(
    "pct_auxilio",
    F.round((F.col("recebeu_auxilio") / F.col("total_entrevistados")) * 100, 2)
).withColumn(
    "variacao_renda_pct",
    F.when(
        F.col("renda_media_habitual") > 0,
        F.round(((F.col("renda_media_efetiva") - F.col("renda_media_habitual")) / F.col("renda_media_habitual")) * 100, 2)
    ).otherwise(0)
)

economico_regiao.write.mode("overwrite").parquet(f"{gold_path}economico_regiao/")

# =====================================================
# GOLD 7: Acesso a Saúde por Perfil Socioeconômico
# =====================================================
# Cruzamento de indicadores de saúde com perfil econômico
acesso_saude = df.filter(F.col("teve_sintomas_covid") == 1).groupBy(
    "plano_saude", "situacao_domicilio"
).agg(
    F.count("*").alias("total_sintomaticos"),
    F.sum(F.when(F.col("procurou_atendimento") == 1, 1).otherwise(0)).alias("procurou_atendimento"),
    F.sum(F.when(F.col("ficou_internado") == 1, 1).otherwise(0)).alias("internados"),
    F.sum(F.when(F.col("fez_teste_covid") == 1, 1).otherwise(0)).alias("fez_teste"),
    F.avg("idade").alias("idade_media"),
    F.sum("peso_pos_estratificacao").alias("populacao_estimada")
)

acesso_saude = acesso_saude.withColumn(
    "plano_saude_desc",
    F.when(F.col("plano_saude") == 1, "Com plano de saúde")
    .when(F.col("plano_saude") == 2, "Sem plano de saúde")
    .otherwise("Não informado")
).withColumn(
    "situacao_desc",
    F.when(F.col("situacao_domicilio") == 1, "Urbano")
    .when(F.col("situacao_domicilio") == 2, "Rural")
    .otherwise("Não informado")
).withColumn(
    "pct_procurou_atendimento",
    F.round((F.col("procurou_atendimento") / F.col("total_sintomaticos")) * 100, 2)
).withColumn(
    "pct_internados",
    F.round((F.col("internados") / F.col("total_sintomaticos")) * 100, 2)
).withColumn(
    "pct_testados",
    F.round((F.col("fez_teste") / F.col("total_sintomaticos")) * 100, 2)
)

acesso_saude.write.mode("overwrite").parquet(f"{gold_path}acesso_saude/")

job.commit()