<#
    18-run-eda-notebook.ps1 - Convert and run EDA notebook
    Regenera sempre para garantir consistência dos outputs
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 18: Run EDA Notebook"

$notebooksDir = Join-Path $PROJECT_ROOT "notebooks"
$reportsDir = Join-Path $PROJECT_ROOT "reports"
$awsProfile = $env:AWS_PROFILE

if (-not (Test-Path $reportsDir)) {
    New-Item -Path $reportsDir -ItemType Directory -Force | Out-Null
}

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

# Verificar autenticação AWS antes de executar consultas Athena no notebook
Write-Info "Validating AWS session..."
$awsIdentityOutput = aws sts get-caller-identity --region $AWS_REGION 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "AWS session validation failed"
    $awsIdentityOutput | Show-LimitedOutput -Lines 15
    if ($awsProfile) {
        Write-Info "Run: aws login --profile $awsProfile"
        Write-Info "Alternative: aws sso login --profile $awsProfile"
    } else {
        Write-Info "Run: aws login"
        Write-Info "Alternative: aws sso login --profile <your-profile>"
    }
    exit 1
}

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
    $jupytextLog = Join-Path $reportsDir "eda_jupytext.log"
    & $jupytextPath --to notebook $edaScript 2>&1 | Tee-Object -FilePath $jupytextLog | Show-LimitedOutput
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed converting .py to .ipynb"
        Write-Info "Full conversion log: $jupytextLog"
        exit 1
    }
}

# Execute notebook using Python module approach (more reliable on Windows)
if (Test-Path $edaNotebook) {
    Write-Info "Executing EDA notebook..."
    $nbconvertLog = Join-Path $reportsDir "eda_nbconvert.log"
    
    # Use python -m instead of jupyter directly for better Windows compatibility
    & $CONDA_PYTHON -m jupyter nbconvert --to notebook --execute $edaNotebook --inplace 2>&1 | Tee-Object -FilePath $nbconvertLog | Show-LimitedOutput -Lines 40
    $nbconvertExitCode = $LASTEXITCODE
    
    if ($nbconvertExitCode -eq 0) {
        Write-Success "Notebook executed successfully"
    } else {
        Write-Error "Notebook execution failed"
        Write-Info "Full execution log: $nbconvertLog"

        $nbconvertRaw = Get-Content -Path $nbconvertLog -Raw -ErrorAction SilentlyContinue
        if ($nbconvertRaw -match "LoginRefreshRequired|refresh token has expired|TOKEN_EXPIRED|aws login|SSO session has expired|AccessDeniedException") {
            Write-Info "Detected expired/invalid AWS credentials while querying Athena."
            if ($awsProfile) {
                Write-Info "Reauthenticate with: aws login --profile $awsProfile"
            } else {
                Write-Info "Reauthenticate with: aws login"
            }
        }

        Write-Info "Try running manually: $CONDA_PYTHON -m jupyter nbconvert --execute $edaNotebook"
        exit 1
    }
} else {
    Write-Error "EDA notebook not found: $edaNotebook"
    exit 1
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
