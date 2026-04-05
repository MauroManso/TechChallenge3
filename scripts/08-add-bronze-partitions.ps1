<#
    08-add-bronze-partitions.ps1 - Add partitions to Bronze table
    Idempotent: Skips partitions that already exist
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 8: Add Partitions to Bronze Table"

$tableName = "pnad_bronze"
# Requisito Tech Challenge: usar apenas 3 meses (Setembro, Outubro, Novembro/2020)
$months = @("09", "10", "11")
$year = "2020"

# Get existing partitions
Write-Info "Checking existing partitions..."
$existingPartitions = aws glue get-partitions --database-name $GLUE_DATABASE --table-name $tableName --query 'Partitions[*].Values' --output json 2>$null | ConvertFrom-Json
$existingPartitionKeys = @()
if ($existingPartitions) {
    foreach ($p in $existingPartitions) {
        $existingPartitionKeys += "$($p[0])/$($p[1])"
    }
}

Write-Info "Found $($existingPartitionKeys.Count) existing partitions"

foreach ($month in $months) {
    $partitionKey = "$year/$month"
    
    if ($existingPartitionKeys -contains $partitionKey) {
        Write-Skipped "Partition year=$year/month=$month"
        continue
    }
    
    Write-Info "Adding partition: year=$year/month=$month"
    
    $partitionInput = @{
        Values = @($year, $month)
        StorageDescriptor = @{
            Location = "s3://$BUCKET_NAME/bronze/year=$year/month=$month/"
            InputFormat = "org.apache.hadoop.mapred.TextInputFormat"
            OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            SerdeInfo = @{
                SerializationLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                Parameters = @{
                    "field.delim" = ";"
                    "skip.header.line.count" = "1"
                }
            }
        }
    } | ConvertTo-Json -Depth 10 -Compress
    
    aws glue create-partition --database-name $GLUE_DATABASE --table-name $tableName --partition-input $partitionInput 2>$null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Added partition: year=$year/month=$month"
    } else {
        # Partition might already exist
        Write-Skipped "Partition year=$year/month=$month (may already exist)"
    }
}

# Verify partitions
Write-Info "Current partitions:"
aws glue get-partitions --database-name $GLUE_DATABASE --table-name $tableName --query 'Partitions[*].Values' --output table 2>$null | Show-LimitedOutput

Write-Success "Bronze partitions setup complete!"
