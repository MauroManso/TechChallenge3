<#
    14-run-silver-to-gold-job.ps1 - Execute Silver to Gold Glue Job
    Idempotent: Checks if gold data already exists before running
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 14: Run Silver to Gold Glue Job"

$jobName = "silver-to-gold-pnad"
$goldPath = "s3://$BUCKET_NAME/gold/"

# Check if gold data already exists
Write-Info "Checking if gold data already exists..."
$goldFiles = aws s3 ls $goldPath --recursive 2>$null | Where-Object { $_ -match "\.parquet$" }

if ($goldFiles) {
    $fileCount = ($goldFiles | Measure-Object -Line).Lines
    Write-Skipped "Gold layer already has $fileCount parquet files"
    Write-Info "To re-run, delete gold data first: aws s3 rm $goldPath --recursive"
    $goldFiles | Show-LimitedOutput -Lines 15
} else {
    # Check if job exists
    if (-not (Test-GlueJobExists -JobName $jobName)) {
        Write-Error "Job '$jobName' does not exist. Run 13-create-silver-to-gold-job.ps1 first"
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
        if ($status -eq "FAILED") {
            aws glue get-job-run --job-name $jobName --run-id $runId --query 'JobRun.ErrorMessage' --output text
        }
        exit 1
    }
    
    # Verify output
    Write-Info "Verifying gold data:"
    aws s3 ls $goldPath --recursive | Show-LimitedOutput -Lines 20
}

Write-Success "Silver to Gold transformation complete!"
