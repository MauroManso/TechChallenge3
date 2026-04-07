"""
Pipeline configuration module.

Centralizes configuration for the pipeline runner, extending src/config.py
with pipeline-specific settings.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SRC_DIR = PROJECT_ROOT / "src"

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("PNAD_BUCKET_NAME") or os.getenv("S3_BUCKET", "pnad-covid-techchallenge3")
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "pnad_covid_db")
GLUE_ROLE_NAME = os.getenv("GLUE_ROLE", "GlueServiceRole-TechChallenge3")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "techchallenge3")

# S3 Paths (constructed from bucket name)
S3_BRONZE = f"s3://{S3_BUCKET}/bronze/"
S3_SILVER = f"s3://{S3_BUCKET}/silver/"
S3_GOLD = f"s3://{S3_BUCKET}/gold/"
S3_SCRIPTS = f"s3://{S3_BUCKET}/scripts/"
S3_ATHENA_RESULTS = f"s3://{S3_BUCKET}/athena-results/"

# Local data paths
DATA_MICRODADOS = DATA_DIR / "microdados" / "dados"
DATA_BRONZE_LOCAL = DATA_DIR / "bronze"

# Glue job configuration
GLUE_JOBS = {
    "bronze-to-silver": {
        "name": "bronze-to-silver-pnad",
        "script": "bronze_to_silver.py",
        "description": "Transform bronze CSV data to silver Parquet format",
    },
    "silver-to-gold": {
        "name": "silver-to-gold-pnad",
        "script": "silver_to_gold.py",
        "description": "Aggregate silver data into gold analytical tables",
    },
}

# Glue crawler configuration  
GLUE_CRAWLERS = {
    "silver": {
        "name": "pnad-silver-crawler",
        "target": S3_SILVER,
    },
    "gold": {
        "name": "pnad-gold-crawler",
        "target": S3_GOLD,
    },
}

# Folders to create in S3 bucket
S3_FOLDERS = ["bronze", "silver", "gold", "scripts", "athena-results"]

# Bronze table schema path
BRONZE_TABLE_SCHEMA = SRC_DIR / "glue" / "create_bronze_table.json"

# Glue scripts to upload
GLUE_SCRIPT_FILES = [
    SRC_DIR / "glue" / "bronze_to_silver.py",
    SRC_DIR / "glue" / "silver_to_gold.py",
]

# Validation queries for Athena
ATHENA_VALIDATION_QUERIES = [
    ("Count gold_evolucao_nacional", "SELECT COUNT(*) as cnt FROM gold_evolucao_nacional"),
    ("Count gold_sintomas_uf_mes", "SELECT COUNT(*) as cnt FROM gold_sintomas_uf_mes"),
    ("Sample gold_evolucao_nacional", "SELECT * FROM gold_evolucao_nacional LIMIT 5"),
]
