<#
    run-all.ps1 - Orchestrator script to run all steps in sequence
    Usage: .\run-all.ps1 [-SkipTo <step>] [-StopAt <step>] [-DryRun]
#>

param(
    [int]$SkipTo = 1,
    [int]$StopAt = 18,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

# Define all steps in order
$steps = @(
    @{ Number = 1;  Script = "01-verify-aws-setup.ps1";         Description = "Verify AWS CLI setup" },
    @{ Number = 2;  Script = "02-create-s3-bucket.ps1";         Description = "Create S3 bucket" },
    @{ Number = 3;  Script = "03-create-glue-database.ps1";     Description = "Create Glue database" },
    @{ Number = 4;  Script = "04-create-iam-role.ps1";          Description = "Create IAM role" },
    @{ Number = 5;  Script = "05-extract-local-data.ps1";       Description = "Extract local data" },
    @{ Number = 6;  Script = "06-upload-bronze-to-s3.ps1";      Description = "Upload bronze to S3" },
    @{ Number = 7;  Script = "07-create-bronze-table.ps1";      Description = "Create bronze table" },
    @{ Number = 8;  Script = "08-add-bronze-partitions.ps1";    Description = "Add bronze partitions" },
    @{ Number = 9;  Script = "09-upload-glue-scripts.ps1";      Description = "Upload Glue scripts" },
    @{ Number = 10; Script = "10-create-bronze-to-silver-job.ps1"; Description = "Create bronze-to-silver job" },
    @{ Number = 11; Script = "11-run-bronze-to-silver-job.ps1"; Description = "Run bronze-to-silver job" },
    @{ Number = 12; Script = "12-create-silver-crawler.ps1";    Description = "Create silver crawler" },
    @{ Number = 13; Script = "13-create-silver-to-gold-job.ps1"; Description = "Create silver-to-gold job" },
    @{ Number = 14; Script = "14-run-silver-to-gold-job.ps1";   Description = "Run silver-to-gold job" },
    @{ Number = 15; Script = "15-create-gold-crawler.ps1";      Description = "Create gold crawler" },
    @{ Number = 16; Script = "16-create-athena-workgroup.ps1";  Description = "Create Athena workgroup" },
    @{ Number = 17; Script = "17-run-athena-queries.ps1";       Description = "Run Athena queries" },
    @{ Number = 18; Script = "18-run-eda-notebook.ps1";         Description = "Run EDA notebook" }
)

# Banner
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║      PNAD COVID-19 - Tech Challenge 3 - Pipeline Runner      ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Show execution plan
Write-Host "Execution Plan:" -ForegroundColor Yellow
Write-Host "  Starting from step: $SkipTo"
Write-Host "  Stopping at step:   $StopAt"
Write-Host ""

if ($DryRun) {
    Write-Host "[DRY RUN MODE - No scripts will be executed]" -ForegroundColor Magenta
    Write-Host ""
}

# Filter steps
$stepsToRun = $steps | Where-Object { $_.Number -ge $SkipTo -and $_.Number -le $StopAt }

Write-Host "Steps to execute:" -ForegroundColor Yellow
foreach ($step in $stepsToRun) {
    Write-Host "  [$($step.Number.ToString().PadLeft(2, '0'))] $($step.Description)"
}
Write-Host ""

if (-not $DryRun) {
    $confirm = Read-Host "Continue? (Y/n)"
    if ($confirm -eq "n" -or $confirm -eq "N") {
        Write-Host "Cancelled."
        exit 0
    }
}

# Track results
$results = @()
$startTime = Get-Date

# Execute each step
foreach ($step in $stepsToRun) {
    $stepStart = Get-Date
    $scriptPath = Join-Path $PSScriptRoot $step.Script
    
    Write-Host ""
    Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║ STEP $($step.Number.ToString().PadLeft(2, '0')): $($step.Description.PadRight(49))║" -ForegroundColor Green
    Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    
    if ($DryRun) {
        Write-Host "[DRY RUN] Would execute: $scriptPath" -ForegroundColor Magenta
        $results += @{
            Step = $step.Number
            Name = $step.Description
            Status = "SKIPPED (dry run)"
            Duration = "0s"
        }
        continue
    }
    
    if (-not (Test-Path $scriptPath)) {
        Write-Host "[ERROR] Script not found: $scriptPath" -ForegroundColor Red
        $results += @{
            Step = $step.Number
            Name = $step.Description
            Status = "NOT FOUND"
            Duration = "0s"
        }
        continue
    }
    
    try {
        # Execute script
        & $scriptPath
        $exitCode = $LASTEXITCODE
        
        $stepEnd = Get-Date
        $duration = ($stepEnd - $stepStart).TotalSeconds
        
        if ($exitCode -eq 0 -or $null -eq $exitCode) {
            Write-Host ""
            Write-Host "[STEP $($step.Number) COMPLETED] Duration: $([math]::Round($duration, 1))s" -ForegroundColor Green
            $results += @{
                Step = $step.Number
                Name = $step.Description
                Status = "SUCCESS"
                Duration = "$([math]::Round($duration, 1))s"
            }
        } else {
            Write-Host ""
            Write-Host "[STEP $($step.Number) FAILED] Exit code: $exitCode" -ForegroundColor Red
            $results += @{
                Step = $step.Number
                Name = $step.Description
                Status = "FAILED (exit $exitCode)"
                Duration = "$([math]::Round($duration, 1))s"
            }
            
            $continueChoice = Read-Host "Continue to next step? (Y/n)"
            if ($continueChoice -eq "n" -or $continueChoice -eq "N") {
                Write-Host "Pipeline stopped by user."
                break
            }
        }
    } catch {
        $stepEnd = Get-Date
        $duration = ($stepEnd - $stepStart).TotalSeconds
        
        Write-Host ""
        Write-Host "[STEP $($step.Number) ERROR] $_" -ForegroundColor Red
        $results += @{
            Step = $step.Number
            Name = $step.Description
            Status = "ERROR"
            Duration = "$([math]::Round($duration, 1))s"
        }
        
        $continueChoice = Read-Host "Continue to next step? (Y/n)"
        if ($continueChoice -eq "n" -or $continueChoice -eq "N") {
            Write-Host "Pipeline stopped by user."
            break
        }
    }
}

# Summary
$endTime = Get-Date
$totalDuration = ($endTime - $startTime).TotalMinutes

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                      EXECUTION SUMMARY                       ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

Write-Host "Step | Status            | Duration | Description"
Write-Host "-----|-------------------|----------|----------------------------------"
foreach ($r in $results) {
    $statusColor = switch -Wildcard ($r.Status) {
        "SUCCESS" { "Green" }
        "FAILED*" { "Red" }
        "ERROR" { "Red" }
        "SKIPPED*" { "Yellow" }
        default { "White" }
    }
    $line = "{0,-4} | {1,-17} | {2,-8} | {3}" -f $r.Step, $r.Status, $r.Duration, $r.Name
    Write-Host $line -ForegroundColor $statusColor
}

Write-Host ""
Write-Host "Total time: $([math]::Round($totalDuration, 1)) minutes" -ForegroundColor Cyan
Write-Host ""

# Count results
$successCount = ($results | Where-Object { $_.Status -eq "SUCCESS" }).Count
$failCount = ($results | Where-Object { $_.Status -like "FAILED*" -or $_.Status -eq "ERROR" }).Count

if ($failCount -eq 0) {
    Write-Host "✓ All $successCount steps completed successfully!" -ForegroundColor Green
} else {
    Write-Host "⚠ $successCount succeeded, $failCount failed" -ForegroundColor Yellow
}
