"""
Step 06: Upload Bronze to S3

Uploads the local bronze CSV files to S3, preserving the partition structure.
"""

import time

from ..aws import S3Adapter
from ..config import AWS_REGION, DATA_BRONZE_LOCAL, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step06UploadBronze(Step):
    """Upload bronze data to S3."""

    @property
    def number(self) -> int:
        return 6

    @property
    def name(self) -> str:
        return "upload-bronze-to-s3"

    @property
    def description(self) -> str:
        return "Upload bronze to S3"

    def check_can_skip(self) -> tuple[bool, str]:
        s3 = S3Adapter(region=AWS_REGION)
        
        # Check if CSV files already exist in S3 bronze
        objects = s3.list_objects(S3_BUCKET, prefix="bronze/", suffix=".csv", max_keys=10)
        if objects:
            return True, f"Bronze data already in S3 ({len(objects)}+ CSV files)"
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
            # Check local files exist
            local_csvs = list(DATA_BRONZE_LOCAL.rglob("*.csv"))
            if not local_csvs:
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message="No local CSV files found. Run step 05 first.",
                )

            s3 = S3Adapter(region=AWS_REGION)

            print(f"  Source: {DATA_BRONZE_LOCAL}")
            print(f"  Target: s3://{S3_BUCKET}/bronze/")
            print(f"  Files to upload: {len(local_csvs)}")

            # Upload files preserving directory structure
            uploaded = s3.upload_directory(
                DATA_BRONZE_LOCAL,
                S3_BUCKET,
                prefix="bronze",
                pattern="*.csv",
                recursive=True,
            )

            print(f"  ✓ Uploaded {len(uploaded)} files")
            for key in uploaded[:5]:
                print(f"    - {key}")
            if len(uploaded) > 5:
                print(f"    ... and {len(uploaded) - 5} more")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Uploaded {len(uploaded)} files to S3",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
