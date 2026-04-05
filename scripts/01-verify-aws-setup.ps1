<#
    01-verify-aws-setup.ps1 - Verify AWS CLI Installation and Configuration
    Idempotent: Safe to run multiple times
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 1: Verify AWS CLI Installation and Configuration"

# Check AWS CLI installation
Write-Info "Checking AWS CLI installation..."
$awsVersion = aws --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Success "AWS CLI installed: $awsVersion"
} else {
    Write-Error "AWS CLI not installed. Run: winget install Amazon.AWSCLI"
    exit 1
}

# Check AWS credentials
Write-Info "Checking AWS credentials..."
$callerIdentity = aws sts get-caller-identity 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Success "AWS credentials configured"
    $callerIdentity | ConvertFrom-Json | Format-List
} else {
    Write-Error "AWS credentials not configured. Run: aws configure"
    exit 1
}

Write-Success "AWS setup verified successfully!"
