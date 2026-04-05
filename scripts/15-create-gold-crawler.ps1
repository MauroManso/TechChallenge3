<#
    15-create-gold-crawler.ps1 - Create and run Gold layer crawler
    Idempotent: Skips if crawler/tables already exist
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 15: Create and Run Gold Layer Crawler"

$crawlerName = "pnad-gold-crawler"

# Get role ARN
$roleArn = aws iam get-role --role-name $GLUE_ROLE_NAME --query 'Role.Arn' --output text
if (-not $roleArn) {
    Write-Error "Could not get IAM role ARN. Run 04-create-iam-role.ps1 first"
    exit 1
}

# Check if crawler exists
Write-Info "Checking if crawler '$crawlerName' exists..."
if (Test-GlueCrawlerExists -CrawlerName $crawlerName) {
    Write-Skipped "Crawler '$crawlerName' already exists"
} else {
    Write-Info "Creating crawler '$crawlerName'..."
    
    $targets = @{
        S3Targets = @(
            @{ Path = "s3://$BUCKET_NAME/gold/" }
        )
    } | ConvertTo-Json -Depth 10 -Compress
    
    aws glue create-crawler `
        --name $crawlerName `
        --role $roleArn `
        --database-name $GLUE_DATABASE `
        --targets $targets `
        --table-prefix "gold_" `
        --region $AWS_REGION
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Crawler created successfully"
    } else {
        Write-Error "Failed to create crawler"
        exit 1
    }
}

# Check if gold tables already exist
$goldTables = aws glue get-tables --database-name $GLUE_DATABASE --region $AWS_REGION --query "TableList[?starts_with(Name, 'gold_')].Name" --output json | ConvertFrom-Json
if ($goldTables -and $goldTables.Count -gt 0) {
    Write-Skipped "Gold tables already exist in catalog: $($goldTables -join ', ')"
} else {
    # Run crawler
    Write-Info "Starting crawler..."
    
    $crawlerState = aws glue get-crawler --name $crawlerName --region $AWS_REGION --query 'Crawler.State' --output text
    if ($crawlerState -eq "RUNNING") {
        Write-Info "Crawler is already running, waiting..."
    } elseif ($crawlerState -eq "READY") {
        aws glue start-crawler --name $crawlerName --region $AWS_REGION
    }
    
    # Wait for completion
    Write-Info "Waiting for crawler to complete..."
    do {
        Start-Sleep -Seconds 15
        $crawlerStatus = aws glue get-crawler --name $crawlerName --region $AWS_REGION --query 'Crawler.State' --output text
        $timestamp = Get-Date -Format "HH:mm:ss"
        Write-Host "[$timestamp] Crawler Gold status: $crawlerStatus"
    } while ($crawlerStatus -eq "RUNNING" -or $crawlerStatus -eq "STARTING" -or $crawlerStatus -eq "STOPPING")
    
    Write-Success "Crawler finished"
}

# Verify all tables
Write-Info "All tables in database:"
aws glue get-tables --database-name $GLUE_DATABASE --region $AWS_REGION --query 'TableList[*].Name' --output table | Show-LimitedOutput

Write-Success "Gold crawler setup complete!"
