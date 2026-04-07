"""
Step 16: Create Athena Workgroup

Creates the Athena workgroup for query execution.
"""

import time

from ..aws import AthenaAdapter
from ..config import ATHENA_WORKGROUP, AWS_REGION, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step16CreateAthenaWorkgroup(Step):
    """Create Athena workgroup for query execution."""

    @property
    def number(self) -> int:
        return 16

    @property
    def name(self) -> str:
        return "create-athena-workgroup"

    @property
    def description(self) -> str:
        return "Create Athena workgroup"

    def check_can_skip(self) -> tuple[bool, str]:
        athena = AthenaAdapter(region=AWS_REGION)
        if athena.workgroup_exists(ATHENA_WORKGROUP):
            return True, f"Workgroup '{ATHENA_WORKGROUP}' already exists"
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
            athena = AthenaAdapter(region=AWS_REGION)
            output_location = f"s3://{S3_BUCKET}/athena-results/"

            print(f"  Workgroup: {ATHENA_WORKGROUP}")
            print(f"  Output location: {output_location}")

            created = athena.create_workgroup(
                workgroup_name=ATHENA_WORKGROUP,
                output_location=output_location,
                description="Tech Challenge 3 Athena workgroup",
                enforce_configuration=True,
                publish_cloudwatch_metrics=True,
            )

            if created:
                print(f"  ✓ Created workgroup: {ATHENA_WORKGROUP}")
            else:
                print(f"  → Workgroup already exists: {ATHENA_WORKGROUP}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Workgroup {ATHENA_WORKGROUP} ready",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
