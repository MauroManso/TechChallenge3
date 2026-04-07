"""
Step 15: Create Gold Crawler

Creates and runs the crawler to catalog the gold layer tables.
"""

import time
from datetime import datetime

from ..aws import GlueAdapter, IAMAdapter
from ..config import AWS_REGION, GLUE_CRAWLERS, GLUE_DATABASE, GLUE_ROLE_NAME, S3_BUCKET
from .base import Step, StepResult, StepStatus


class Step15CreateGoldCrawler(Step):
    """Create and run gold layer crawler."""

    @property
    def number(self) -> int:
        return 15

    @property
    def name(self) -> str:
        return "create-gold-crawler"

    @property
    def description(self) -> str:
        return "Create gold crawler"

    def check_can_skip(self) -> tuple[bool, str]:
        glue = GlueAdapter(region=AWS_REGION)
        
        # Check if gold tables already exist (crawler already ran)
        # Look for any gold_ prefixed table
        tables = glue.list_tables(GLUE_DATABASE)
        gold_tables = [t for t in tables if t.startswith("gold_")]
        if gold_tables:
            return True, f"Gold tables already exist: {', '.join(gold_tables[:3])}..."
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

            crawler_config = GLUE_CRAWLERS["gold"]
            crawler_name = crawler_config["name"]

            print(f"  Crawler: {crawler_name}")
            print(f"  Target: s3://{S3_BUCKET}/gold/")

            # Create crawler if it doesn't exist
            created = glue.create_crawler(
                crawler_name=crawler_name,
                role_arn=role_arn,
                database_name=GLUE_DATABASE,
                s3_targets=[f"s3://{S3_BUCKET}/gold/"],
                description="Crawls gold layer analytical tables",
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

            # List gold tables
            tables = glue.list_tables(GLUE_DATABASE)
            gold_tables = [t for t in tables if t.startswith("gold_")]
            print(f"  Gold tables: {gold_tables}")

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
