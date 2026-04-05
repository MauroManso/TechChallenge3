<#
    11-run-bronze-to-silver-job.ps1 - Execute Bronze to Silver Glue Job
    Idempotent: Checks if silver data already exists before running
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 11: Run Bronze to Silver Glue Job"

$jobName = "bronze-to-silver-pnad"
$silverPath = "s3://$BUCKET_NAME/silver/"

# Check if silver data already exists
Write-Info "Checking if silver data already exists..."
$silverFiles = aws s3 ls $silverPath --recursive 2>$null | Where-Object { $_ -match "\.parquet$" }

if ($silverFiles) {
    $fileCount = ($silverFiles | Measure-Object -Line).Lines
    Write-Skipped "Silver layer already has $fileCount parquet files"
    Write-Info "To re-run, delete silver data first: aws s3 rm $silverPath --recursive"
    $silverFiles | Show-LimitedOutput -Lines 15
} else {
    # Check if job exists
    if (-not (Test-GlueJobExists -JobName $jobName)) {
        Write-Error "Job '$jobName' does not exist. Run 10-create-bronze-to-silver-job.ps1 first"
        exit 1
    }
    
    # Start job
    Write-Info "Starting job '$jobName'..."
    $runId = aws glue start-job-run --job-name $jobName --query 'JobRunId' --output text
    
    if (-not $runId) {
        Write-Error "Failed to start job"
        exit 1
    }
    
    Write-Info "Job started with RunId: $runId"
    
    # Monitor job
    Write-Info "Monitoring job status (this may take several minutes)..."
    do {
        Start-Sleep -Seconds 30
        $status = aws glue get-job-run --job-name $jobName --run-id $runId --query 'JobRun.JobRunState' --output text
        $timestamp = Get-Date -Format "HH:mm:ss"
        Write-Host "[$timestamp] Status: $status"
    } while ($status -eq "RUNNING" -or $status -eq "STARTING" -or $status -eq "STOPPING")
    
    if ($status -eq "SUCCEEDED") {
        Write-Success "Job completed successfully!"
    } else {
        Write-Error "Job finished with status: $status"
        # Get error message if failed
        if ($status -eq "FAILED") {
            aws glue get-job-run --job-name $jobName --run-id $runId --query 'JobRun.ErrorMessage' --output text
        }
        exit 1
    }
    
    # Verify output
    Write-Info "Verifying silver data:"
    aws s3 ls $silverPath --recursive | Show-LimitedOutput -Lines 20
}

Write-Success "Bronze to Silver transformation complete!"
