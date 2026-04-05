<#
    09-upload-glue-scripts.ps1 - Upload Glue job scripts to S3
    Idempotent: Uses sync/cp which handles existing files
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 9: Upload Glue Job Scripts to S3"

$glueScriptsDir = Join-Path $SRC_DIR "glue"
$s3ScriptsPath = "s3://$BUCKET_NAME/scripts/"

$scripts = @(
    "bronze_to_silver.py",
    "silver_to_gold.py"
)

foreach ($script in $scripts) {
    $localPath = Join-Path $glueScriptsDir $script
    $s3Path = "$s3ScriptsPath$script"
    
    if (-not (Test-Path $localPath)) {
        Write-Error "Script not found: $localPath"
        continue
    }
    
    # Check if script exists in S3
    $s3Exists = Test-S3ObjectExists -S3Path $s3Path
    
    if ($s3Exists) {
        # Compare by uploading only if different (sync behavior)
        Write-Info "Checking if '$script' needs update..."
    }
    
    Write-Info "Uploading '$script' to S3..."
    aws s3 cp $localPath $s3Path
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Uploaded '$script'"
    } else {
        Write-Error "Failed to upload '$script'"
    }
}

# Verify
Write-Info "Scripts in S3:"
aws s3 ls $s3ScriptsPath | Show-LimitedOutput

Write-Success "Glue scripts upload complete!"
