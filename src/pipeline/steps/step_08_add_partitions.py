"""
Step 08: Add Bronze Partitions

Adds year/month partitions to the bronze table.
"""

import time

from ..aws import GlueAdapter, S3Adapter
from ..config import AWS_REGION, GLUE_DATABASE, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step08AddPartitions(Step):
    """Add partitions to bronze table."""

    @property
    def number(self) -> int:
        return 8

    @property
    def name(self) -> str:
        return "add-bronze-partitions"

    @property
    def description(self) -> str:
        return "Add bronze partitions"

    def check_can_skip(self) -> tuple[bool, str]:
        # Always try to add partitions - Glue handles duplicates gracefully
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
            glue = GlueAdapter(region=AWS_REGION)

            # Discover partitions from S3
            objects = s3.list_objects(S3_BUCKET, prefix="bronze/", max_keys=1000)
            
            # Extract unique partition paths
            partitions = set()
            for obj in objects:
                key = obj["Key"]
                # Look for pattern like bronze/year=2020/month=09/
                parts = key.split("/")
                for i, part in enumerate(parts):
                    if part.startswith("year="):
                        year = part.split("=")[1]
                        # Look for month in next part
                        if i + 1 < len(parts) and parts[i + 1].startswith("month="):
                            month = parts[i + 1].split("=")[1]
                            partitions.add((year, month))

            if not partitions:
                print("  ⚠ No partitions discovered from S3")
                print("  Check that data exists in bronze/ with year=YYYY/month=MM structure")
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message="No partitions found in S3",
                )

            print(f"  Discovered {len(partitions)} partitions")

            added = 0
            existed = 0
            for year, month in sorted(partitions):
                location = f"s3://{S3_BUCKET}/bronze/year={year}/month={month}/"
                
                created = glue.add_partition(
                    GLUE_DATABASE,
                    "pnad_bronze",
                    [year, month],
                    location,
                )
                
                if created:
                    print(f"  ✓ Added partition: year={year}/month={month}")
                    added += 1
                else:
                    print(f"  → Partition exists: year={year}/month={month}")
                    existed += 1

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Added {added} partitions ({existed} already existed)",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
