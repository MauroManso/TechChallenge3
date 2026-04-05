<#
    02-create-s3-bucket.ps1 - Create S3 Bucket with folder structure
    Idempotent: Skips if bucket already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 2: Create S3 Bucket and Folder Structure"

# Check if bucket exists
Write-Info "Checking if bucket '$BUCKET_NAME' exists..."
if (Test-S3BucketExists -BucketName $BUCKET_NAME) {
    Write-Skipped "Bucket 's3://$BUCKET_NAME' already exists"
} else {
    Write-Info "Creating bucket 's3://$BUCKET_NAME' in region '$AWS_REGION'..."
    aws s3 mb "s3://$BUCKET_NAME" --region $AWS_REGION
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Bucket created successfully"
    } else {
        Write-Error "Failed to create bucket"
        exit 1
    }
}

# Create folder structure (idempotent - put-object is safe to repeat)
$folders = @("bronze/", "silver/", "gold/", "scripts/", "athena-results/", "spark-logs/")

Write-Info "Ensuring folder structure exists..."
foreach ($folder in $folders) {
    $folderPath = "s3://$BUCKET_NAME/$folder"
    # Check if any object exists with this prefix
    $exists = aws s3 ls $folderPath 2>$null
    if ($exists) {
        Write-Skipped "Folder '$folder'"
    } else {
        aws s3api put-object --bucket $BUCKET_NAME --key $folder | Out-Null
        Write-Success "Created folder '$folder'"
    }
}

# Verify structure
Write-Info "Current bucket structure:"
aws s3 ls "s3://$BUCKET_NAME/" | Show-LimitedOutput

Write-Success "S3 bucket setup complete!"
