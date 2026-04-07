"""
Step 09: Upload Glue Scripts

Uploads the Glue ETL scripts to S3.
"""

import time

from ..aws import S3Adapter
from ..config import AWS_REGION, GLUE_SCRIPT_FILES, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step09UploadScripts(Step):
    """Upload Glue ETL scripts to S3."""

    @property
    def number(self) -> int:
        return 9

    @property
    def name(self) -> str:
        return "upload-glue-scripts"

    @property
    def description(self) -> str:
        return "Upload Glue scripts"

    def check_can_skip(self) -> tuple[bool, str]:
        s3 = S3Adapter(region=AWS_REGION)
        
        # Check if all scripts exist in S3
        all_exist = True
        for script_path in GLUE_SCRIPT_FILES:
            key = f"scripts/{script_path.name}"
            if not s3.object_exists(S3_BUCKET, key):
                all_exist = False
                break
        
        if all_exist:
            return True, f"All {len(GLUE_SCRIPT_FILES)} Glue scripts already in S3"
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
            s3 = S3Adapter(region=AWS_REGION)

            print(f"  Target: s3://{S3_BUCKET}/scripts/")
            
            uploaded = 0
            for script_path in GLUE_SCRIPT_FILES:
                if not script_path.exists():
                    print(f"  ⚠ Script not found: {script_path}")
                    continue
                
                key = f"scripts/{script_path.name}"
                s3.upload_file(script_path, S3_BUCKET, key)
                print(f"  ✓ Uploaded: {script_path.name}")
                uploaded += 1

            if uploaded == 0:
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message="No scripts were uploaded",
                )

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Uploaded {uploaded} scripts",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
