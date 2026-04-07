"""
Step 02: Create S3 Bucket

Creates the S3 bucket with the required folder structure for the data lake.
"""

import time

from ..aws import S3Adapter
from ..config import AWS_REGION, S3_BUCKET, S3_FOLDERS
from .base import Step, StepResult, StepStatus


class Step02CreateBucket(Step):
    """Create S3 bucket with data lake folder structure."""

    @property
    def number(self) -> int:
        return 2

    @property
    def name(self) -> str:
        return "create-s3-bucket"

    @property
    def description(self) -> str:
        return "Create S3 bucket"

    def check_can_skip(self) -> tuple[bool, str]:
        s3 = S3Adapter(region=AWS_REGION)
        if s3.bucket_exists(S3_BUCKET):
            return True, f"Bucket '{S3_BUCKET}' already exists"
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

            # Create bucket
            created = s3.create_bucket(S3_BUCKET)
            if created:
                print(f"  ✓ Created bucket: {S3_BUCKET}")
            else:
                print(f"  → Bucket already exists: {S3_BUCKET}")

            # Create folder structure
            created_folders = s3.create_folders(S3_BUCKET, S3_FOLDERS)
            for folder in created_folders:
                print(f"  ✓ Created folder: {folder}/")
            
            existing_folders = set(S3_FOLDERS) - set(created_folders)
            for folder in existing_folders:
                print(f"  → Folder exists: {folder}/")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Bucket {S3_BUCKET} ready with {len(S3_FOLDERS)} folders",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
