<#
    10-create-bronze-to-silver-job.ps1 - Create Glue Job for Bronze to Silver transformation
    Idempotent: Deletes and recreates job to ensure latest script is used
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 10: Create Bronze to Silver Glue Job"

$jobName = "bronze-to-silver-pnad"

# ALWAYS delete existing job to ensure fresh script is used (no caching)
Write-Info "Checking if job '$jobName' exists..."
if (Test-GlueJobExists -JobName $jobName) {
    Write-Info "Deleting existing job to ensure fresh script..."
    aws glue delete-job --job-name $jobName
    Start-Sleep -Seconds 2
    Write-Success "Deleted old job"
}

Write-Info "Creating Glue job '$jobName'..."

# Get role ARN
$roleArn = aws iam get-role --role-name $GLUE_ROLE_NAME --query 'Role.Arn' --output text
if (-not $roleArn) {
    Write-Error "Could not get IAM role ARN. Run 04-create-iam-role.ps1 first"
    exit 1
}

$command = @{
    Name = "glueetl"
    ScriptLocation = "s3://$BUCKET_NAME/scripts/bronze_to_silver.py"
    PythonVersion = "3"
} | ConvertTo-Json -Compress

$defaultArgs = @{
    "--job-language" = "python"
    "--S3_BUCKET" = $BUCKET_NAME
    "--enable-metrics" = "true"
    "--enable-spark-ui" = "true"
    "--spark-event-logs-path" = "s3://$BUCKET_NAME/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
} | ConvertTo-Json -Compress

aws glue create-job `
    --name $jobName `
    --role $roleArn `
    --command $command `
    --default-arguments $defaultArgs `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

if ($LASTEXITCODE -eq 0) {
    Write-Success "Job created successfully"
} else {
    Write-Error "Failed to create job"
    exit 1
}

Write-Success "Bronze to Silver job setup complete!"
