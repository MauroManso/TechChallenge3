"""
Utilitário para executar queries no Athena via CLI.
"""
import subprocess
import json
import time
from pathlib import Path


def run_athena_query(query: str, database: str = "pnad_covid_db", workgroup: str = "techchallenge3") -> dict:
    """
    Executa query no Athena e retorna resultados.
    """
    # Iniciar execução
    start_cmd = [
        "aws", "athena", "start-query-execution",
        "--query-string", query,
        "--work-group", workgroup,
        "--query-execution-context", json.dumps({"Database": database})
    ]
    
    result = subprocess.run(start_cmd, capture_output=True, text=True)
    execution_id = json.loads(result.stdout)["QueryExecutionId"]
    print(f"Query iniciada: {execution_id}")
    
    # Aguardar conclusão
    while True:
        status_cmd = [
            "aws", "athena", "get-query-execution",
            "--query-execution-id", execution_id
        ]
        status_result = subprocess.run(status_cmd, capture_output=True, text=True)
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
        "--query-execution-id", execution_id
    ]
    results = subprocess.run(results_cmd, capture_output=True, text=True)
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