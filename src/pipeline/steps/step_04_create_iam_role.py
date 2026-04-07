"""
Step 04: Create IAM Role

Creates the IAM role and policies required for Glue jobs.
"""

import time

from ..aws import IAMAdapter
from ..config import AWS_REGION, GLUE_ROLE_NAME, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step04CreateIAMRole(Step):
    """Create IAM role for Glue service."""

    @property
    def number(self) -> int:
        return 4

    @property
    def name(self) -> str:
        return "create-iam-role"

    @property
    def description(self) -> str:
        return "Create IAM role"

    def check_can_skip(self) -> tuple[bool, str]:
        iam = IAMAdapter(region=AWS_REGION)
        if iam.role_exists(GLUE_ROLE_NAME):
            return True, f"IAM role '{GLUE_ROLE_NAME}' already exists"
        return False, ""

    def run(self, dry_run: bool = False) -> StepResult:
        start_time = time.time()

        if dry_run:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SKIPPED,
                duration_seconds=0.0,
                message="dry run",
            )

        try:
            iam = IAMAdapter(region=AWS_REGION)

            created, arn = iam.create_glue_service_role(
                GLUE_ROLE_NAME,
                s3_bucket=S3_BUCKET,
            )

            if created:
                print(f"  ✓ Created role: {GLUE_ROLE_NAME}")
                print(f"  ✓ ARN: {arn}")
                print(f"  ✓ Attached: AWSGlueServiceRole")
                print(f"  ✓ Attached: AmazonS3FullAccess")
            else:
                print(f"  → Role already exists: {GLUE_ROLE_NAME}")
                print(f"  → ARN: {arn}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Role {GLUE_ROLE_NAME} ready",
                details={"arn": arn},
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
