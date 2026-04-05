<#
    16-create-athena-workgroup.ps1 - Create Athena Workgroup
    Idempotent: Skips if workgroup already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 16: Create Athena Workgroup"

# Check if workgroup exists
Write-Info "Checking if workgroup '$ATHENA_WORKGROUP' exists..."
if (Test-AthenaWorkgroupExists -WorkgroupName $ATHENA_WORKGROUP) {
    Write-Skipped "Workgroup '$ATHENA_WORKGROUP' already exists"
    aws athena get-work-group --work-group $ATHENA_WORKGROUP --query 'WorkGroup.{Name:Name,State:State}' --output table
} else {
    Write-Info "Creating workgroup '$ATHENA_WORKGROUP'..."
    
    $configuration = @{
        ResultConfiguration = @{
            OutputLocation = "s3://$BUCKET_NAME/athena-results/"
        }
        EnforceWorkGroupConfiguration = $false
    } | ConvertTo-Json -Depth 10 -Compress
    
    aws athena create-work-group `
        --name $ATHENA_WORKGROUP `
        --configuration $configuration
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Workgroup created successfully"
    } else {
        Write-Error "Failed to create workgroup"
        exit 1
    }
}

Write-Success "Athena workgroup setup complete!"
