"""
Utilitário para executar queries no Athena via CLI.
"""
import subprocess
import json
import time
import os
from pathlib import Path


def run_athena_query(query: str, database: str = "pnad_covid_db", workgroup: str = "techchallenge3", region: str = None) -> dict:
    """
    Executa query no Athena e retorna resultados.
    """
    # Use region from parameter, environment, or default to us-east-1
    region = region or os.environ.get("AWS_REGION", "us-east-1")
    
    # Iniciar execução
    start_cmd = [
        "aws", "athena", "start-query-execution",
        "--query-string", query,
        "--work-group", workgroup,
        "--query-execution-context", json.dumps({"Database": database}),
        "--region", region
    ]
    
    result = subprocess.run(start_cmd, capture_output=True, text=True)
    
    # Check for errors
    if result.returncode != 0:
        raise Exception(f"AWS CLI error: {result.stderr}")
    
    if not result.stdout.strip():
        raise Exception(f"Empty response from AWS CLI. stderr: {result.stderr}")
    
    execution_id = json.loads(result.stdout)["QueryExecutionId"]
    print(f"Query iniciada: {execution_id}")
    
    # Aguardar conclusão
    while True:
        status_cmd = [
            "aws", "athena", "get-query-execution",
            "--query-execution-id", execution_id,
            "--region", region
        ]
        status_result = subprocess.run(status_cmd, capture_output=True, text=True)
        
        if status_result.returncode != 0:
            raise Exception(f"Failed to get query status: {status_result.stderr}")
        
        status = json.loads(status_result.stdout)["QueryExecution"]["Status"]["State"]
        
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        print(f"Status: {status}")
        time.sleep(2)
    
    if status != "SUCCEEDED":
        error = json.loads(status_result.stdout)["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise Exception(f"Query falhou: {error}")
    
    # Obter resultados
    results_cmd = [
        "aws", "athena", "get-query-results",
        "--query-execution-id", execution_id,
        "--region", region
    ]
    results = subprocess.run(results_cmd, capture_output=True, text=True)
    
    if results.returncode != 0:
        raise Exception(f"Failed to get query results: {results.stderr}")
    
    return json.loads(results.stdout)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        query = sys.argv[1]
    else:
        query = "SELECT * FROM gold_evolucao_nacional LIMIT 10"
    
    results = run_athena_query(query)
    
    # Formatar saída
    rows = results.get("ResultSet", {}).get("Rows", [])
    if rows:
        headers = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
        print(" | ".join(headers))
        print("-" * 80)
        for row in rows[1:]:
            values = [col.get("VarCharValue", "") for col in row.get("Data", [])]
            print(" | ".join(values))