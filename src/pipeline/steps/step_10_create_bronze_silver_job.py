"""
Step 10: Create Bronze-to-Silver Job

Creates the Glue job that transforms bronze CSV data to silver Parquet.
"""

import time
from datetime import datetime

from ..aws import GlueAdapter, IAMAdapter
from ..config import AWS_REGION, GLUE_DATABASE, GLUE_JOBS, GLUE_ROLE_NAME, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step10CreateBronzeSilverJob(Step):
    """Create bronze-to-silver Glue job."""

    @property
    def number(self) -> int:
        return 10

    @property
    def name(self) -> str:
        return "create-bronze-to-silver-job"

    @property
    def description(self) -> str:
        return "Create bronze-to-silver job"

    def check_can_skip(self) -> tuple[bool, str]:
        glue = GlueAdapter(region=AWS_REGION)
        job_name = GLUE_JOBS["bronze-to-silver"]["name"]
        if glue.job_exists(job_name):
            return True, f"Job '{job_name}' already exists"
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
            glue = GlueAdapter(region=AWS_REGION)

            # Get role ARN
            role_arn = iam.get_role_arn(GLUE_ROLE_NAME)
            if not role_arn:
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message=f"IAM role '{GLUE_ROLE_NAME}' not found. Run step 04 first.",
                )

            job_config = GLUE_JOBS["bronze-to-silver"]
            job_name = job_config["name"]
            script_location = f"s3://{S3_BUCKET}/scripts/{job_config['script']}"

            print(f"  Job name: {job_name}")
            print(f"  Script: {script_location}")
            print(f"  Role: {GLUE_ROLE_NAME}")

            created = glue.create_job(
                job_name=job_name,
                role_arn=role_arn,
                script_location=script_location,
                description=job_config["description"],
                glue_version="4.0",
                worker_type="G.1X",
                number_of_workers=2,
                default_arguments={
                    "--source_database": GLUE_DATABASE,
                    "--source_table": "pnad_bronze",
                    "--output_path": f"s3://{S3_BUCKET}/silver/",
                },
                timeout=60,
            )

            if created:
                print(f"  ✓ Created job: {job_name}")
            else:
                print(f"  → Job already exists: {job_name}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Job {job_name} ready",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
