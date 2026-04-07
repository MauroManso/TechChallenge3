"""
Step 03: Create Glue Database

Creates the Glue Data Catalog database for the pipeline.
"""

import time

from ..aws import GlueAdapter
from ..config import AWS_REGION, GLUE_DATABASE
from .base import Step, StepResult, StepStatus


class Step03CreateGlueDB(Step):
    """Create Glue Data Catalog database."""

    @property
    def number(self) -> int:
        return 3

    @property
    def name(self) -> str:
        return "create-glue-database"

    @property
    def description(self) -> str:
        return "Create Glue database"

    def check_can_skip(self) -> tuple[bool, str]:
        glue = GlueAdapter(region=AWS_REGION)
        if glue.database_exists(GLUE_DATABASE):
            return True, f"Database '{GLUE_DATABASE}' already exists"
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
            glue = GlueAdapter(region=AWS_REGION)

            created = glue.create_database(
                GLUE_DATABASE,
                description="PNAD COVID-19 Tech Challenge 3 database",
            )

            if created:
                print(f"  ✓ Created database: {GLUE_DATABASE}")
            else:
                print(f"  → Database already exists: {GLUE_DATABASE}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Database {GLUE_DATABASE} ready",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
