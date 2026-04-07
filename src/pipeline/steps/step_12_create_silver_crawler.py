"""
Step 12: Create Silver Crawler

Creates and runs the crawler to catalog the silver layer.
"""

import time
from datetime import datetime

from ..aws import GlueAdapter, IAMAdapter
from ..config import AWS_REGION, GLUE_CRAWLERS, GLUE_DATABASE, GLUE_ROLE_NAME, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step12CreateSilverCrawler(Step):
    """Create and run silver layer crawler."""

    @property
    def number(self) -> int:
        return 12

    @property
    def name(self) -> str:
        return "create-silver-crawler"

    @property
    def description(self) -> str:
        return "Create silver crawler"

    def check_can_skip(self) -> tuple[bool, str]:
        glue = GlueAdapter(region=AWS_REGION)
        
        # Check if silver table already exists (crawler already ran)
        if glue.table_exists(GLUE_DATABASE, "silver"):
            return True, "Silver table already exists in catalog"
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
                    message=f"IAM role '{GLUE_ROLE_NAME}' not found.",
                )

            crawler_config = GLUE_CRAWLERS["silver"]
            crawler_name = crawler_config["name"]

            print(f"  Crawler: {crawler_name}")
            print(f"  Target: s3://{S3_BUCKET}/silver/")

            # Create crawler if it doesn't exist
            created = glue.create_crawler(
                crawler_name=crawler_name,
                role_arn=role_arn,
                database_name=GLUE_DATABASE,
                s3_targets=[f"s3://{S3_BUCKET}/silver/"],
                description="Crawls silver layer parquet data",
            )

            if created:
                print(f"  ✓ Created crawler: {crawler_name}")
            else:
                print(f"  → Crawler already exists: {crawler_name}")

            # Run crawler and wait
            print("  Starting crawler...")

            def progress_callback(status: str, elapsed: int) -> None:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"  [{timestamp}] Crawler status: {status}")

            result = glue.run_crawler_and_wait(
                crawler_name=crawler_name,
                poll_interval=15,
                timeout=600,  # 10 minutes
                progress_callback=progress_callback,
            )

            print(f"  ✓ Crawler finished")
            if result.tables_created > 0:
                print(f"  Tables created: {result.tables_created}")
            if result.tables_updated > 0:
                print(f"  Tables updated: {result.tables_updated}")

            # List tables
            tables = glue.list_tables(GLUE_DATABASE)
            print(f"  Tables in {GLUE_DATABASE}: {tables}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Crawler complete, {result.tables_created} tables created",
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
