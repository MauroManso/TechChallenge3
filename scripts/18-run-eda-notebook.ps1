<#
    18-run-eda-notebook.ps1 - Convert and run EDA notebook
    Regenera sempre para garantir consistência dos outputs
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 18: Run EDA Notebook"

$notebooksDir = Join-Path $PROJECT_ROOT "notebooks"
$reportsDir = Join-Path $PROJECT_ROOT "reports"

# Derivar caminhos de ferramentas do ambiente Conda
$envDir = Split-Path $CONDA_PYTHON -Parent
$jupytextPath = Join-Path $envDir "Scripts\jupytext.exe"
$jupyterPath = Join-Path $envDir "Scripts\jupyter.exe"

# Verificar se ferramentas existem
if (-not (Test-Path $jupytextPath)) { 
    Write-Info "jupytext not found at $jupytextPath, trying system PATH"
    $jupytextPath = "jupytext" 
}
if (-not (Test-Path $jupyterPath)) { 
    Write-Info "jupyter not found at $jupyterPath, trying system PATH"
    $jupyterPath = "jupyter" 
}

Write-Info "Using Python: $CONDA_PYTHON"
Write-Info "Using jupytext: $jupytextPath"
Write-Info "Using jupyter: $jupyterPath"

# Check if notebook/script exists
$edaScript = Join-Path $notebooksDir "01_eda_pnad_covid.py"
$edaNotebook = Join-Path $notebooksDir "01_eda_pnad_covid.ipynb"

if (-not (Test-Path $edaScript) -and -not (Test-Path $edaNotebook)) {
    Write-Error "EDA notebook/script not found in: $notebooksDir"
    Write-Info "Create either 01_eda_pnad_covid.py or 01_eda_pnad_covid.ipynb"
    exit 1
}

# Sempre regenerar para garantir consistência (governança)
Write-Info "Regenerando EDA para garantir consistência com dados atuais..."

# Convert .py to .ipynb if needed
if (Test-Path $edaScript) {
    Write-Info "Converting .py to .ipynb..."
    & $jupytextPath --to notebook $edaScript 2>&1 | Show-LimitedOutput
}

# Execute notebook using Python module approach (more reliable on Windows)
if (Test-Path $edaNotebook) {
    Write-Info "Executing EDA notebook..."
    
    # Use python -m instead of jupyter directly for better Windows compatibility
    & $CONDA_PYTHON -m jupyter nbconvert --to notebook --execute $edaNotebook --inplace 2>&1 | Show-LimitedOutput
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Notebook executed successfully"
    } else {
        Write-Error "Notebook execution failed"
        Write-Info "Try running manually: $CONDA_PYTHON -m jupyter nbconvert --execute $edaNotebook"
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
