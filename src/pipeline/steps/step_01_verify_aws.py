"""
Step 01: Verify AWS Setup

Verifies that AWS credentials are properly configured and accessible.
"""

import time

from ..aws import STSAdapter
from ..config import AWS_REGION
from .base import Step, StepResult, StepStatus


class Step01VerifyAWS(Step):
    """Verify AWS CLI/credentials are properly configured."""

    @property
    def number(self) -> int:
        return 1

    @property
    def name(self) -> str:
        return "verify-aws-setup"

    @property
    def description(self) -> str:
        return "Verify AWS CLI setup"

    def check_can_skip(self) -> tuple[bool, str]:
        # This step should always run to verify credentials
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
            sts = STSAdapter(region=AWS_REGION)
            is_valid, message = sts.verify_credentials()

            if is_valid:
                print(f"  ✓ {message}")
                print(f"  ✓ Region: {AWS_REGION}")
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    message=message,
                )
            else:
                print(f"  ✗ {message}")
                print("  Try: aws configure  OR  aws sso login")
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message=message,
                )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
