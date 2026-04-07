"""
Step 11: Run Bronze-to-Silver Job

Runs the Glue job and waits for completion with polling.
"""

import time
from datetime import datetime

from ..aws import GlueAdapter, S3Adapter
from ..config import AWS_REGION, GLUE_JOBS, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step11RunBronzeSilverJob(Step):
    """Run bronze-to-silver Glue job."""

    @property
    def number(self) -> int:
        return 11

    @property
    def name(self) -> str:
        return "run-bronze-to-silver-job"

    @property
    def description(self) -> str:
        return "Run bronze-to-silver job"

    def check_can_skip(self) -> tuple[bool, str]:
        s3 = S3Adapter(region=AWS_REGION)
        
        # Check if silver parquet files already exist
        objects = s3.list_objects(S3_BUCKET, prefix="silver/", suffix=".parquet", max_keys=10)
        if objects:
            return True, f"Silver data already exists ({len(objects)}+ parquet files)"
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
            job_name = GLUE_JOBS["bronze-to-silver"]["name"]

            # Check job exists
            if not glue.job_exists(job_name):
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message=f"Job '{job_name}' not found. Run step 10 first.",
                )

            print(f"  Starting job: {job_name}")
            print("  Monitoring (this may take several minutes)...")

            def progress_callback(status: str, elapsed: int) -> None:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"  [{timestamp}] Status: {status}")

            result = glue.run_job_and_wait(
                job_name=job_name,
                poll_interval=30,
                timeout=1800,  # 30 minutes
                progress_callback=progress_callback,
            )

            if result.state.value == "SUCCEEDED":
                print(f"  ✓ Job completed successfully")
                print(f"  Execution time: {result.execution_time}s")
                
                # Verify output
                s3 = S3Adapter(region=AWS_REGION)
                silver_files = s3.list_objects(S3_BUCKET, prefix="silver/", suffix=".parquet", max_keys=20)
                print(f"  Silver files created: {len(silver_files)}")
                
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    message=f"Job succeeded in {result.execution_time}s",
                )
            else:
                error_msg = result.error_message or f"Job ended with status: {result.state.value}"
                print(f"  ✗ {error_msg}")
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message=error_msg,
                )

        except TimeoutError as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.FAILED,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
