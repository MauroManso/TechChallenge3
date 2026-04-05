<#
    06-upload-bronze-to-s3.ps1 - Upload bronze data to S3
    Idempotent: Uses sync which only uploads changed/new files
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 6: Upload Bronze Data to S3"

$bronzeLocalPath = Join-Path $DATA_DIR "bronze"
$bronzeS3Path = "s3://$BUCKET_NAME/bronze/"

# Check if local bronze directory exists
if (-not (Test-Path $bronzeLocalPath)) {
    Write-Error "Local bronze directory not found: $bronzeLocalPath"
    Write-Info "Run 05-extract-local-data.ps1 first"
    exit 1
}

# Check current S3 state
Write-Info "Checking current S3 bronze state..."
$s3Files = aws s3 ls $bronzeS3Path --recursive 2>$null
if ($s3Files) {
    $s3FileCount = ($s3Files | Measure-Object -Line).Lines
    Write-Info "Found $s3FileCount files already in S3"
}

# Sync (only uploads new/changed files)
Write-Info "Syncing local bronze to S3 (only new/changed files)..."
aws s3 sync $bronzeLocalPath $bronzeS3Path --exclude "*.gitkeep" --exclude ".gitkeep"

if ($LASTEXITCODE -eq 0) {
    Write-Success "Sync complete"
} else {
    Write-Error "Sync failed"
    exit 1
}

# Verify
Write-Info "Current S3 bronze contents:"
aws s3 ls $bronzeS3Path --recursive | Show-LimitedOutput -Lines 20

Write-Success "Bronze data upload complete!"
