"""
Step 07: Create Bronze Table

Creates the Glue catalog table for the bronze layer.
"""

import time

from ..aws import GlueAdapter
from ..config import AWS_REGION, BRONZE_TABLE_SCHEMA, GLUE_DATABASE
from .base import Step, StepResult, StepStatus


class Step07CreateBronzeTable(Step):
    """Create bronze table in Glue catalog."""

    @property
    def number(self) -> int:
        return 7

    @property
    def name(self) -> str:
        return "create-bronze-table"

    @property
    def description(self) -> str:
        return "Create bronze table"

    def check_can_skip(self) -> tuple[bool, str]:
        glue = GlueAdapter(region=AWS_REGION)
        if glue.table_exists(GLUE_DATABASE, "pnad_bronze"):
            return True, "Bronze table 'pnad_bronze' already exists"
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
            # Check schema file exists
            if not BRONZE_TABLE_SCHEMA.exists():
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.ERROR,
                    duration_seconds=time.time() - start_time,
                    message=f"Schema file not found: {BRONZE_TABLE_SCHEMA}",
                )

            glue = GlueAdapter(region=AWS_REGION)

            print(f"  Database: {GLUE_DATABASE}")
            print(f"  Schema: {BRONZE_TABLE_SCHEMA.name}")

            created = glue.create_table_from_json(GLUE_DATABASE, str(BRONZE_TABLE_SCHEMA))

            if created:
                print(f"  ✓ Created table: pnad_bronze")
            else:
                print(f"  → Table already exists: pnad_bronze")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message="Bronze table ready",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
