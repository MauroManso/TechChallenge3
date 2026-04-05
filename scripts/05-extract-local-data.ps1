<#
    05-extract-local-data.ps1 - Extract microdados from ZIP files locally
    Idempotent: Skips if CSV files already exist
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 5: Extract Local Data (Microdados from ZIPs)"

$bronzeDir = Join-Path $DATA_DIR "bronze"

# Check if bronze directory has CSV files
$existingCsvs = Get-ChildItem -Path $bronzeDir -Recurse -Filter "*.csv" -ErrorAction SilentlyContinue
if ($existingCsvs -and $existingCsvs.Count -gt 0) {
    Write-Skipped "Local bronze data already extracted ($($existingCsvs.Count) CSV files found)"
    $existingCsvs | Select-Object Name, Length, LastWriteTime | Format-Table | Out-String | Show-LimitedOutput -Lines 15
} else {
    Write-Info "Running extraction script..."
    
    $extractScript = Join-Path $SRC_DIR "data\extract_microdados.py"
    if (Test-Path $extractScript) {
        Write-Info "Using Python: $CONDA_PYTHON"
        & $CONDA_PYTHON $extractScript
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Data extraction complete"
            Get-ChildItem -Path $bronzeDir -Recurse -Filter "*.csv" | Select-Object Name, Length | Format-Table
        } else {
            Write-Error "Extraction script failed"
            exit 1
        }
    } else {
        Write-Error "Extraction script not found at: $extractScript"
        Write-Info "Please create the script first or extract manually"
        exit 1
    }
}

Write-Success "Local data extraction complete!"
