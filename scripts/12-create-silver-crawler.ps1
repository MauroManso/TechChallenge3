<#
    12-create-silver-crawler.ps1 - Create and run Silver layer crawler
    Idempotent: Skips if crawler/table already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 12: Create and Run Silver Layer Crawler"

$crawlerName = "pnad-silver-crawler"

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
            @{ Path = "s3://$BUCKET_NAME/silver/" }
        )
    } | ConvertTo-Json -Depth 10 -Compress
    
    aws glue create-crawler `
        --name $crawlerName `
        --role $roleArn `
        --database-name $GLUE_DATABASE `
        --targets $targets
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Crawler created successfully"
    } else {
        Write-Error "Failed to create crawler"
        exit 1
    }
}

# Check if silver table already exists (crawler already ran successfully)
$silverTableExists = Test-GlueTableExists -DatabaseName $GLUE_DATABASE -TableName "silver"
if ($silverTableExists) {
    Write-Skipped "Silver table already exists in catalog"
} else {
    # Run crawler
    Write-Info "Starting crawler..."
    
    # Check crawler state first
    $crawlerState = aws glue get-crawler --name $crawlerName --query 'Crawler.State' --output text
    if ($crawlerState -eq "RUNNING") {
        Write-Info "Crawler is already running, waiting..."
    } elseif ($crawlerState -eq "READY") {
        aws glue start-crawler --name $crawlerName
    }
    
    # Wait for completion
    Write-Info "Waiting for crawler to complete..."
    do {
        Start-Sleep -Seconds 15
        $crawlerStatus = aws glue get-crawler --name $crawlerName --query 'Crawler.State' --output text
        $timestamp = Get-Date -Format "HH:mm:ss"
        Write-Host "[$timestamp] Crawler status: $crawlerStatus"
    } while ($crawlerStatus -eq "RUNNING" -or $crawlerStatus -eq "STARTING" -or $crawlerStatus -eq "STOPPING")
    
    Write-Success "Crawler finished"
}

# Verify tables
Write-Info "Tables in database:"
aws glue get-tables --database-name $GLUE_DATABASE --query 'TableList[*].Name' --output table | Show-LimitedOutput

Write-Success "Silver crawler setup complete!"
