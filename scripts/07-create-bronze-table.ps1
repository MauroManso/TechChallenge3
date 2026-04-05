<#
    07-create-bronze-table.ps1 - Create Bronze Table in Glue Catalog
    Idempotent: Skips if table already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 7: Create Bronze Table in Glue Catalog"

$tableName = "pnad_bronze"

# Check if table exists
Write-Info "Checking if table '$tableName' exists..."
if (Test-GlueTableExists -DatabaseName $GLUE_DATABASE -TableName $tableName) {
    Write-Skipped "Table '$tableName' already exists"
    aws glue get-table --database-name $GLUE_DATABASE --name $tableName --region $AWS_REGION --query 'Table.{Name:Name,TableType:TableType,Location:StorageDescriptor.Location}' --output table
} else {
    Write-Info "Creating table '$tableName'..."
    
    # Check if JSON template exists
    $tableJsonPath = Join-Path $SRC_DIR "glue\create_bronze_table.json"
    if (Test-Path $tableJsonPath) {
        # Update bucket name in JSON and create table
        $tableJson = Get-Content $tableJsonPath -Raw
        $tableJson = $tableJson -replace "BUCKET_NAME", $BUCKET_NAME
        $tempFile = Join-Path $env:TEMP "create_bronze_table_temp.json"
        $tableJson | Out-File -FilePath $tempFile -Encoding utf8
        
        aws glue create-table --cli-input-json "file://$tempFile" --region $AWS_REGION
        Remove-Item $tempFile -ErrorAction SilentlyContinue
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Table created successfully"
        } else {
            Write-Error "Failed to create table"
            exit 1
        }
    } else {
        Write-Error "Table JSON template not found at: $tableJsonPath"
        Write-Info "Please create the JSON template first"
        exit 1
    }
}

Write-Success "Bronze table setup complete!"
