<#
    04-create-iam-role.ps1 - Create IAM Role for Glue Jobs
    Idempotent: Skips if role already exists
#>

# Load configuration
. "$PSScriptRoot\00-config.ps1"

Write-Step "Step 4: Create IAM Role for Glue Jobs"

# Check if role exists
Write-Info "Checking if IAM role '$GLUE_ROLE_NAME' exists..."
if (Test-IAMRoleExists -RoleName $GLUE_ROLE_NAME) {
    Write-Skipped "IAM Role '$GLUE_ROLE_NAME' already exists"
    aws iam get-role --role-name $GLUE_ROLE_NAME --query 'Role.{RoleName:RoleName,Arn:Arn,CreateDate:CreateDate}' --output table
} else {
    Write-Info "Creating IAM role '$GLUE_ROLE_NAME'..."
    
    # Create trust policy
    $trustPolicy = @{
        Version = "2012-10-17"
        Statement = @(
            @{
                Effect = "Allow"
                Principal = @{
                    Service = "glue.amazonaws.com"
                }
                Action = "sts:AssumeRole"
            }
        )
    } | ConvertTo-Json -Depth 10 -Compress
    
    # Create role
    aws iam create-role --role-name $GLUE_ROLE_NAME --assume-role-policy-document $trustPolicy
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create IAM role"
        exit 1
    }
    
    Write-Success "IAM role created"
}

# Attach policies (idempotent - attach is safe to repeat)
Write-Info "Ensuring required policies are attached..."

$policies = @(
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess"
)

foreach ($policyArn in $policies) {
    $policyName = $policyArn.Split("/")[-1]
    # Check if policy is attached
    $attachedPolicies = aws iam list-attached-role-policies --role-name $GLUE_ROLE_NAME --query "AttachedPolicies[?PolicyArn=='$policyArn'].PolicyArn" --output text 2>$null
    if ($attachedPolicies) {
        Write-Skipped "Policy '$policyName' already attached"
    } else {
        aws iam attach-role-policy --role-name $GLUE_ROLE_NAME --policy-arn $policyArn
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Attached policy '$policyName'"
        } else {
            Write-Error "Failed to attach policy '$policyName'"
        }
    }
}

Write-Success "IAM role setup complete!"
