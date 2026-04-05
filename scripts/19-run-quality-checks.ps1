<#
    19-run-quality-checks.ps1 - Run data quality checks
    Validates data quality after ETL completion
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 19: Run Data Quality Checks"

$qualityScript = Join-Path $SRC_DIR "data\run_quality_checks.py"

# Check if script exists
if (-not (Test-Path $qualityScript)) {
    Write-Error "Quality check runner not found: $qualityScript"
    exit 1
}

Write-Info "Running quality checks..."
Write-Info "Using Python: $CONDA_PYTHON"
& $CONDA_PYTHON $qualityScript 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Success "Quality checks passed!"
} else {
    Write-Error "Quality checks failed!"
    exit 1
}

Write-Success "Quality checks complete!"
