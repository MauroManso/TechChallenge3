<#
    17-run-athena-queries.ps1 - Run sample Athena queries
    Idempotent: Safe to run multiple times (queries are read-only)
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 17: Run Sample Athena Queries"

# Check if run_query.py exists
$queryScript = Join-Path $SRC_DIR "athena\run_query.py"
if (-not (Test-Path $queryScript)) {
    Write-Error "Query helper script not found: $queryScript"
    Write-Info "Please create src/athena/run_query.py first"
    exit 1
}

# Sample queries
$queries = @(
    @{
        Name = "Evolução temporal dos sintomas"
        Query = @"
SELECT 
    mes,
    total_entrevistados,
    total_com_sintomas,
    pct_sintomaticos,
    total_testados,
    total_positivos
FROM gold_evolucao_nacional
ORDER BY mes
LIMIT 20
"@
    },
    @{
        Name = "Top 10 UFs com mais sintomáticos"
        Query = @"
SELECT 
    uf_nome,
    regiao,
    SUM(total_com_sintomas_covid) as total_sintomaticos,
    ROUND(AVG(pct_sintomas_covid), 2) as media_pct_sintomas
FROM gold_sintomas_uf_mes
GROUP BY uf_nome, regiao
ORDER BY total_sintomaticos DESC
LIMIT 10
"@
    }
)

foreach ($q in $queries) {
    Write-Info "Running query: $($q.Name)"
    Write-Host "-" * 60
    
    try {
        python $queryScript $q.Query 2>&1 | Show-LimitedOutput -Lines 25
    } catch {
        Write-Error "Query failed: $_"
    }
    
    Write-Host ""
}

Write-Success "Athena queries complete!"
