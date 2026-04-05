<#
    99-cleanup-aws-resources.ps1 - Clean up all AWS resources
    WARNING: This will DELETE all resources created by this project!
    Idempotent: Safe to run multiple times (skips non-existent resources)
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "CLEANUP: Delete All AWS Resources"

Write-Host ""
Write-Host "WARNING: This will DELETE all AWS resources for this project!" -ForegroundColor Red
Write-Host "This includes: S3 bucket, Glue database/tables/jobs/crawlers, IAM role, Athena workgroup" -ForegroundColor Red
Write-Host ""

$confirmation = Read-Host "Type 'DELETE' to confirm"
if ($confirmation -ne "DELETE") {
    Write-Info "Cleanup cancelled"
    exit 0
}

# Delete Glue Jobs
Write-Info "Deleting Glue Jobs..."
$jobs = @("bronze-to-silver-pnad", "silver-to-gold-pnad")
foreach ($job in $jobs) {
    if (Test-GlueJobExists -JobName $job) {
        aws glue delete-job --job-name $job
        Write-Success "Deleted job: $job"
    } else {
        Write-Skipped "Job '$job' does not exist"
    }
}

# Delete Glue Crawlers
Write-Info "Deleting Glue Crawlers..."
$crawlers = @("pnad-silver-crawler", "pnad-gold-crawler")
foreach ($crawler in $crawlers) {
    if (Test-GlueCrawlerExists -CrawlerName $crawler) {
        aws glue delete-crawler --name $crawler
        Write-Success "Deleted crawler: $crawler"
    } else {
        Write-Skipped "Crawler '$crawler' does not exist"
    }
}

# Delete Glue Tables
Write-Info "Deleting Glue Tables..."
$tables = @(
    "pnad_bronze",
    "silver",
    "gold_sintomas_uf_mes",
    "gold_trabalho_regiao_mes",
    "gold_perfil_sintomaticos",
    "gold_testes_uf",
    "gold_evolucao_nacional",
    "gold_acesso_saude"
)
foreach ($table in $tables) {
    if (Test-GlueTableExists -DatabaseName $GLUE_DATABASE -TableName $table) {
        aws glue delete-table --database-name $GLUE_DATABASE --name $table
        Write-Success "Deleted table: $table"
    } else {
        Write-Skipped "Table '$table' does not exist"
    }
}

# Delete Glue Database
Write-Info "Deleting Glue Database..."
if (Test-GlueDatabaseExists -DatabaseName $GLUE_DATABASE) {
    aws glue delete-database --name $GLUE_DATABASE
    Write-Success "Deleted database: $GLUE_DATABASE"
} else {
    Write-Skipped "Database '$GLUE_DATABASE' does not exist"
}

# Empty and delete S3 bucket
Write-Info "Deleting S3 Bucket..."
if (Test-S3BucketExists -BucketName $BUCKET_NAME) {
    Write-Info "Emptying bucket (this may take a while)..."
    aws s3 rm "s3://$BUCKET_NAME" --recursive 2>&1 | Show-LimitedOutput
    aws s3 rb "s3://$BUCKET_NAME"
    Write-Success "Deleted bucket: $BUCKET_NAME"
} else {
    Write-Skipped "Bucket '$BUCKET_NAME' does not exist"
}

# Delete Athena Workgroup
Write-Info "Deleting Athena Workgroup..."
if (Test-AthenaWorkgroupExists -WorkgroupName $ATHENA_WORKGROUP) {
    aws athena delete-work-group --work-group $ATHENA_WORKGROUP --recursive-delete-option
    Write-Success "Deleted workgroup: $ATHENA_WORKGROUP"
} else {
    Write-Skipped "Workgroup '$ATHENA_WORKGROUP' does not exist"
}

# Delete IAM Role
Write-Info "Deleting IAM Role..."
if (Test-IAMRoleExists -RoleName $GLUE_ROLE_NAME) {
    # Detach policies first
    aws iam detach-role-policy --role-name $GLUE_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>$null
    aws iam detach-role-policy --role-name $GLUE_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess 2>$null
    aws iam delete-role --role-name $GLUE_ROLE_NAME
    Write-Success "Deleted IAM role: $GLUE_ROLE_NAME"
} else {
    Write-Skipped "IAM role '$GLUE_ROLE_NAME' does not exist"
}

# Verify cleanup
Write-Step "Verifying Cleanup"
Write-Info "Checking for remaining resources..."

Write-Host "S3 buckets with 'pnad':"
aws s3 ls 2>$null | Select-String "pnad" | Show-LimitedOutput

Write-Host "Glue databases:"
aws glue get-databases --query 'DatabaseList[*].Name' 2>$null | Show-LimitedOutput

Write-Host "IAM roles with 'GlueServiceRole':"
aws iam list-roles --query "Roles[?contains(RoleName, 'GlueServiceRole')].RoleName" 2>$null | Show-LimitedOutput

Write-Success "Cleanup complete!"
