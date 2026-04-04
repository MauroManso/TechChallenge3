"""
Configurações centralizadas do projeto Tech Challenge 3.

Carrega variáveis de ambiente do arquivo .env e define constantes
para uso nos scripts de ingestão, transformação e análise.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Diretório raiz do projeto
PROJECT_ROOT = Path(__file__).parent.parent

# Configurações AWS
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "pnad-covid-techchallenge3")
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "pnad_covid_db")
GLUE_ROLE = os.getenv("GLUE_ROLE", "GlueServiceRole-TechChallenge3")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "techchallenge3")

# Paths S3 (camadas de dados)
S3_BRONZE = f"s3://{S3_BUCKET}/bronze/"
S3_SILVER = f"s3://{S3_BUCKET}/silver/"
S3_GOLD = f"s3://{S3_BUCKET}/gold/"
S3_SCRIPTS = f"s3://{S3_BUCKET}/scripts/"
S3_ATHENA_RESULTS = f"s3://{S3_BUCKET}/athena-results/"

# Paths locais
DATA_DIR = PROJECT_ROOT / "data"
DATA_MICRODADOS = DATA_DIR / "microdados" / "dados"
DATA_DOCUMENTACAO = DATA_DIR / "microdados" / "documentacao"
DATA_MENSAL = DATA_DIR / "mensal" / "tabelas"
DATA_BRONZE_LOCAL = DATA_DIR / "bronze"

# Configurações de processamento
SPARK_CONFIG = {
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.shuffle.partitions": "10",
}
