<#
    18-run-eda-notebook.ps1 - Convert and run EDA notebook
    Idempotent: Safe to run multiple times
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 18: Run EDA Notebook"

$notebooksDir = Join-Path $PROJECT_ROOT "notebooks"
$reportsDir = Join-Path $PROJECT_ROOT "reports"

# Check if notebook/script exists
$edaScript = Join-Path $notebooksDir "01_eda_pnad_covid.py"
$edaNotebook = Join-Path $notebooksDir "01_eda_pnad_covid.ipynb"

if (-not (Test-Path $edaScript) -and -not (Test-Path $edaNotebook)) {
    Write-Error "EDA notebook/script not found in: $notebooksDir"
    Write-Info "Create either 01_eda_pnad_covid.py or 01_eda_pnad_covid.ipynb"
    exit 1
}

# Check if reports already exist
$existingReports = Get-ChildItem -Path $reportsDir -Filter "*.png" -ErrorAction SilentlyContinue
if ($existingReports -and $existingReports.Count -gt 0) {
    Write-Skipped "EDA reports already exist ($($existingReports.Count) PNG files)"
    $existingReports | Select-Object Name, LastWriteTime | Format-Table
} else {
    # Convert .py to .ipynb if needed
    if (Test-Path $edaScript) {
        Write-Info "Converting .py to .ipynb..."
        jupytext --to notebook $edaScript 2>&1 | Show-LimitedOutput
    }
    
    # Execute notebook
    if (Test-Path $edaNotebook) {
        Write-Info "Executing EDA notebook..."
        jupyter nbconvert --to notebook --execute $edaNotebook --inplace 2>&1 | Show-LimitedOutput
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Notebook executed successfully"
        } else {
            Write-Error "Notebook execution failed"
        }
    }
}

# Verify reports
Write-Info "Generated reports:"
Get-ChildItem -Path $reportsDir -Filter "*.png" -ErrorAction SilentlyContinue | 
    Select-Object Name, Length, LastWriteTime | 
    Format-Table | 
    Out-String | 
    Show-LimitedOutput

Write-Success "EDA notebook complete!"
exit 0
