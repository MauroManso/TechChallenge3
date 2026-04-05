<#
    03-create-glue-database.ps1 - Create Glue Data Catalog Database
    Idempotent: Skips if database already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 3: Create Glue Data Catalog Database"

# Check if database exists
Write-Info "Checking if database '$GLUE_DATABASE' exists..."
if (Test-GlueDatabaseExists -DatabaseName $GLUE_DATABASE) {
    Write-Skipped "Database '$GLUE_DATABASE' already exists"
    aws glue get-database --name $GLUE_DATABASE --query 'Database.{Name:Name,Description:Description}' --output table
} else {
    Write-Info "Creating database '$GLUE_DATABASE'..."
    $databaseInput = @{
        Name = $GLUE_DATABASE
        Description = "Database para dados PNAD COVID-19 - Tech Challenge 3"
    } | ConvertTo-Json -Compress
    
    aws glue create-database --database-input $databaseInput
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Database created successfully"
    } else {
        Write-Error "Failed to create database"
        exit 1
    }
}

Write-Success "Glue database setup complete!"
