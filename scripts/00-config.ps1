<# 
    00-config.ps1 - Configuration Variables
    Shared configuration for all scripts
#>

# ============================================
# CONFIGURATION - Edit these values as needed
# ============================================

# AWS Region
$script:AWS_REGION = "us-east-1"

# Bucket name - set this to your actual bucket name
# If you already created a bucket, put its name here
$script:BUCKET_NAME = $env:PNAD_BUCKET_NAME
if (-not $script:BUCKET_NAME) {
    # Try to find existing bucket
    $existingBucket = aws s3 ls 2>$null | Select-String "pnad-covid-techchallenge3" | ForEach-Object { $_.Line.Split()[-1] } | Select-Object -First 1
    if ($existingBucket) {
        $script:BUCKET_NAME = $existingBucket
    } else {
        $script:BUCKET_NAME = "pnad-covid-techchallenge3"
    }
}

# Glue configuration
$script:GLUE_DATABASE = "pnad_covid_db"
$script:GLUE_ROLE_NAME = "GlueServiceRole-TechChallenge3"

# Athena configuration
$script:ATHENA_WORKGROUP = "techchallenge3"

# Project paths
$script:PROJECT_ROOT = Split-Path -Parent $PSScriptRoot
$script:DATA_DIR = Join-Path $script:PROJECT_ROOT "data"
$script:SRC_DIR = Join-Path $script:PROJECT_ROOT "src"

# ============================================
# HELPER FUNCTIONS
# ============================================

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host " $Message" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Skipped {
    param([string]$Message)
    Write-Host "[SKIP] $Message (already exists)" -ForegroundColor DarkGray
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Show-LimitedOutput {
    param(
        [Parameter(ValueFromPipeline=$true)]
        [string[]]$InputObject,
        [int]$Lines = 20
    )
    begin { $lineCount = 0; $collected = @() }
    process {
        foreach ($line in $InputObject) {
            $collected += $line
            $lineCount++
        }
    }
    end {
        $collected | Select-Object -First $Lines
        if ($lineCount -gt $Lines) {
            Write-Host "... (showing $Lines of $lineCount lines)" -ForegroundColor DarkGray
        }
    }
}

function Test-S3ObjectExists {
    param([string]$S3Path)
    $result = aws s3 ls $S3Path --region $script:AWS_REGION 2>$null
    return ($null -ne $result -and $result -ne "")
}

function Test-S3BucketExists {
    param([string]$BucketName)
    $result = aws s3 ls "s3://$BucketName" --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-GlueDatabaseExists {
    param([string]$DatabaseName)
    $result = aws glue get-database --name $DatabaseName --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-GlueTableExists {
    param([string]$DatabaseName, [string]$TableName)
    $result = aws glue get-table --database-name $DatabaseName --name $TableName --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-GlueJobExists {
    param([string]$JobName)
    $result = aws glue get-job --job-name $JobName --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-GlueCrawlerExists {
    param([string]$CrawlerName)
    $result = aws glue get-crawler --name $CrawlerName --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-IAMRoleExists {
    param([string]$RoleName)
    $result = aws iam get-role --role-name $RoleName 2>&1
    return $LASTEXITCODE -eq 0
}

function Test-AthenaWorkgroupExists {
    param([string]$WorkgroupName)
    $result = aws athena get-work-group --work-group $WorkgroupName --region $script:AWS_REGION 2>&1
    return $LASTEXITCODE -eq 0
}

function Get-CondaPythonPath {
    param([string]$EnvName = "techchallenge3")
    
    # Tentar caminhos comuns para ambientes Conda
    $possiblePaths = @(
        "$env:USERPROFILE\.conda\envs\$EnvName\python.exe",
        "$env:USERPROFILE\miniconda3\envs\$EnvName\python.exe",
        "$env:USERPROFILE\anaconda3\envs\$EnvName\python.exe",
        "C:\ProgramData\miniconda3\envs\$EnvName\python.exe",
        "C:\ProgramData\anaconda3\envs\$EnvName\python.exe"
    )
    
    $pythonPath = $possiblePaths | Where-Object { Test-Path $_ } | Select-Object -First 1
    
    if ($pythonPath) {
        return $pythonPath
    }
    
    # Fallback para python do sistema
    return "python"
}

# Variável global para Python do ambiente
$script:CONDA_PYTHON = Get-CondaPythonPath

# Export variables
Write-Host "Configuration loaded:" -ForegroundColor Green
Write-Host "  BUCKET_NAME: $script:BUCKET_NAME"
Write-Host "  AWS_REGION: $script:AWS_REGION"
Write-Host "  GLUE_DATABASE: $script:GLUE_DATABASE"
Write-Host "  PROJECT_ROOT: $script:PROJECT_ROOT"
Write-Host "  CONDA_PYTHON: $script:CONDA_PYTHON"
