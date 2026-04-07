"""
Pipeline runner - main orchestrator for the PNAD COVID pipeline.

This module replaces run-all.ps1, preserving the same operator controls
and execution semantics while using Python and boto3.
"""

import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from .cli import (
    PipelineArgs,
    confirm_execution,
    parse_args,
    print_banner,
    print_step_banner,
)
from .steps.base import Step, StepResult, StepStatus
from .steps.step_01_verify_aws import Step01VerifyAWS
from .steps.step_02_create_bucket import Step02CreateBucket
from .steps.step_03_create_glue_db import Step03CreateGlueDB
from .steps.step_04_create_iam_role import Step04CreateIAMRole
from .steps.step_05_extract_data import Step05ExtractData
from .steps.step_06_upload_bronze import Step06UploadBronze
from .steps.step_07_create_bronze_table import Step07CreateBronzeTable
from .steps.step_08_add_partitions import Step08AddPartitions
from .steps.step_09_upload_scripts import Step09UploadScripts
from .steps.step_10_create_bronze_silver_job import Step10CreateBronzeSilverJob
from .steps.step_11_run_bronze_silver_job import Step11RunBronzeSilverJob
from .steps.step_12_create_silver_crawler import Step12CreateSilverCrawler
from .steps.step_13_create_silver_gold_job import Step13CreateSilverGoldJob
from .steps.step_14_run_silver_gold_job import Step14RunSilverGoldJob
from .steps.step_15_create_gold_crawler import Step15CreateGoldCrawler
from .steps.step_16_create_athena_workgroup import Step16CreateAthenaWorkgroup
from .steps.step_17_run_athena_queries import Step17RunAthenaQueries
from .steps.step_18_run_notebook import Step18RunNotebook
from .steps.step_19_quality_checks import Step19QualityChecks

if TYPE_CHECKING:
    from collections.abc import Sequence


# ANSI color codes for terminal output
class Colors:
    """ANSI color codes for terminal output."""
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    MAGENTA = "\033[95m"
    GRAY = "\033[90m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def color(text: str, color_code: str) -> str:
    """Wrap text in ANSI color codes."""
    return f"{color_code}{text}{Colors.RESET}"


def write_info(message: str) -> None:
    """Print an info message."""
    print(color(f"[INFO] {message}", Colors.YELLOW))


def write_success(message: str) -> None:
    """Print a success message."""
    print(color(f"[OK] {message}", Colors.GREEN))


def write_skipped(message: str) -> None:
    """Print a skipped message."""
    print(color(f"[SKIP] {message}", Colors.GRAY))


def write_error(message: str) -> None:
    """Print an error message."""
    print(color(f"[ERROR] {message}", Colors.RED))


@dataclass
class StepDefinition:
    """Definition of a pipeline step."""
    number: int
    name: str
    description: str
    step_class: type[Step] | None = None


# Step registry - maps step numbers to step implementations
STEP_DEFINITIONS: list[StepDefinition] = [
    StepDefinition(1, "01-verify-aws-setup", "Verify AWS CLI setup", Step01VerifyAWS),
    StepDefinition(2, "02-create-s3-bucket", "Create S3 bucket", Step02CreateBucket),
    StepDefinition(3, "03-create-glue-database", "Create Glue database", Step03CreateGlueDB),
    StepDefinition(4, "04-create-iam-role", "Create IAM role", Step04CreateIAMRole),
    StepDefinition(5, "05-extract-local-data", "Extract local data", Step05ExtractData),
    StepDefinition(6, "06-upload-bronze-to-s3", "Upload bronze to S3", Step06UploadBronze),
    StepDefinition(7, "07-create-bronze-table", "Create bronze table", Step07CreateBronzeTable),
    StepDefinition(8, "08-add-bronze-partitions", "Add bronze partitions", Step08AddPartitions),
    StepDefinition(9, "09-upload-glue-scripts", "Upload Glue scripts", Step09UploadScripts),
    StepDefinition(10, "10-create-bronze-to-silver-job", "Create bronze-to-silver job", Step10CreateBronzeSilverJob),
    StepDefinition(11, "11-run-bronze-to-silver-job", "Run bronze-to-silver job", Step11RunBronzeSilverJob),
    StepDefinition(12, "12-create-silver-crawler", "Create silver crawler", Step12CreateSilverCrawler),
    StepDefinition(13, "13-create-silver-to-gold-job", "Create silver-to-gold job", Step13CreateSilverGoldJob),
    StepDefinition(14, "14-run-silver-to-gold-job", "Run silver-to-gold job", Step14RunSilverGoldJob),
    StepDefinition(15, "15-create-gold-crawler", "Create gold crawler", Step15CreateGoldCrawler),
    StepDefinition(16, "16-create-athena-workgroup", "Create Athena workgroup", Step16CreateAthenaWorkgroup),
    StepDefinition(17, "17-run-athena-queries", "Run Athena queries", Step17RunAthenaQueries),
    StepDefinition(18, "18-run-eda-notebook", "Run EDA notebook", Step18RunNotebook),
    StepDefinition(19, "19-run-quality-checks", "Run quality checks", Step19QualityChecks),
]


def get_step_by_number(number: int) -> StepDefinition | None:
    """Get step definition by number."""
    for step_def in STEP_DEFINITIONS:
        if step_def.number == number:
            return step_def
    return None


def print_execution_plan(args: PipelineArgs, steps: list[StepDefinition]) -> None:
    """Print the execution plan."""
    print(color("Execution Plan:", Colors.YELLOW))
    print(f"  Starting from step: {args.skip_to}")
    print(f"  Stopping at step:   {args.stop_at}")
    print()
    
    if args.dry_run:
        print(color("[DRY RUN MODE - No actions will be executed]", Colors.MAGENTA))
        print()
    
    print(color("Steps to execute:", Colors.YELLOW))
    for step in steps:
        print(f"  [{step.number:02d}] {step.description}")
    print()


def print_summary(results: list[StepResult], total_duration_minutes: float) -> None:
    """Print execution summary."""
    print()
    print(color("╔══════════════════════════════════════════════════════════════╗", Colors.CYAN))
    print(color("║                      EXECUTION SUMMARY                       ║", Colors.CYAN))
    print(color("╚══════════════════════════════════════════════════════════════╝", Colors.CYAN))
    print()
    
    print("Step | Status            | Duration | Description")
    print("-----|-------------------|----------|----------------------------------")
    
    for r in results:
        if r.status == StepStatus.SUCCESS:
            status_color = Colors.GREEN
        elif r.status in (StepStatus.FAILED, StepStatus.ERROR):
            status_color = Colors.RED
        elif r.status == StepStatus.SKIPPED:
            status_color = Colors.YELLOW
        else:
            status_color = Colors.RESET
        
        status_str = r.status.value.upper()
        if r.status == StepStatus.FAILED and r.message:
            status_str = f"FAILED"
        
        duration_str = f"{r.duration_seconds:.1f}s"
        line = f"{r.step_number:<4} | {status_str:<17} | {duration_str:<8} | {r.name}"
        print(color(line, status_color))
    
    print()
    print(color(f"Total time: {total_duration_minutes:.1f} minutes", Colors.CYAN))
    print()
    
    # Count results
    success_count = sum(1 for r in results if r.status == StepStatus.SUCCESS)
    skip_count = sum(1 for r in results if r.status == StepStatus.SKIPPED)
    fail_count = sum(1 for r in results if r.status in (StepStatus.FAILED, StepStatus.ERROR))
    
    if fail_count == 0:
        print(color(f"✓ All {success_count + skip_count} steps completed successfully!", Colors.GREEN))
    else:
        print(color(f"⚠ {success_count} succeeded, {skip_count} skipped, {fail_count} failed", Colors.YELLOW))


def prompt_continue_on_failure() -> bool:
    """Prompt user whether to continue after a failure."""
    try:
        response = input("Continue to next step? (Y/n) ").strip().lower()
        return response != "n"
    except (EOFError, KeyboardInterrupt):
        print("\nPipeline stopped by user.")
        return False


def run_pipeline(args: PipelineArgs) -> int:
    """
    Run the pipeline with the given arguments.
    
    Args:
        args: Parsed command-line arguments.
        
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    print_banner()
    
    # Filter steps to run
    steps_to_run = [
        s for s in STEP_DEFINITIONS 
        if args.skip_to <= s.number <= args.stop_at
    ]
    
    print_execution_plan(args, steps_to_run)
    
    # Confirm execution (unless --yes or --dry-run)
    if not args.dry_run and not args.yes:
        if not confirm_execution(args.skip_to, args.stop_at, len(steps_to_run)):
            print("Cancelled.")
            return 0
    
    # Track results
    results: list[StepResult] = []
    start_time = time.time()
    
    # Execute each step
    for step_def in steps_to_run:
        step_start = time.time()
        print_step_banner(step_def.number, step_def.description)
        
        if args.dry_run:
            print(color(f"[DRY RUN] Would execute: {step_def.name}", Colors.MAGENTA))
            results.append(StepResult(
                step_number=step_def.number,
                name=step_def.description,
                status=StepStatus.SKIPPED,
                duration_seconds=0.0,
                message="dry run",
            ))
            continue
        
        # Check if step class is implemented
        if step_def.step_class is None:
            write_error(f"Step {step_def.number} ({step_def.name}) not yet implemented")
            duration = time.time() - step_start
            results.append(StepResult(
                step_number=step_def.number,
                name=step_def.description,
                status=StepStatus.ERROR,
                duration_seconds=duration,
                message="not implemented",
            ))
            
            if not prompt_continue_on_failure():
                break
            continue
        
        # Instantiate and run step
        try:
            step = step_def.step_class()
            
            # Check idempotency
            can_skip, skip_reason = step.check_can_skip()
            if can_skip:
                write_skipped(skip_reason)
                duration = time.time() - step_start
                results.append(StepResult(
                    step_number=step_def.number,
                    name=step_def.description,
                    status=StepStatus.SKIPPED,
                    duration_seconds=duration,
                    message=skip_reason,
                ))
                continue
            
            # Execute step
            result = step.run(dry_run=args.dry_run)
            results.append(result)
            
            if result.success:
                duration = time.time() - step_start
                print()
                write_success(f"STEP {step_def.number} COMPLETED - Duration: {duration:.1f}s")
            else:
                print()
                write_error(f"STEP {step_def.number} FAILED: {result.message}")
                
                if not prompt_continue_on_failure():
                    break
                    
        except Exception as e:
            duration = time.time() - step_start
            print()
            write_error(f"STEP {step_def.number} ERROR: {e}")
            results.append(StepResult(
                step_number=step_def.number,
                name=step_def.description,
                status=StepStatus.ERROR,
                duration_seconds=duration,
                message=str(e),
            ))
            
            if not prompt_continue_on_failure():
                break
    
    # Print summary
    total_duration = (time.time() - start_time) / 60.0
    print_summary(results, total_duration)
    
    # Return exit code based on results
    fail_count = sum(1 for r in results if r.status in (StepStatus.FAILED, StepStatus.ERROR))
    return 1 if fail_count > 0 else 0


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    args = parse_args(argv)
    return run_pipeline(args)


if __name__ == "__main__":
    sys.exit(main())
