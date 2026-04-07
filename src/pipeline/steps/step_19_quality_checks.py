"""
Step 19: Run Quality Checks

Executes quality validation checks on the pipeline output.
"""

import time

from ..config import PROJECT_ROOT
from .base import Step, StepResult, StepStatus


class Step19QualityChecks(Step):
    """Run quality validation checks."""

    @property
    def number(self) -> int:
        return 19

    @property
    def name(self) -> str:
        return "run-quality-checks"

    @property
    def description(self) -> str:
        return "Run quality checks"

    def check_can_skip(self) -> tuple[bool, str]:
        # Always run quality checks
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
            # Import and run the quality checks module
            from ...data.run_quality_checks import main as run_quality_main

            print("  Running quality validations...")
            print("  " + "=" * 50)

            # Run the quality checks
            exit_code = run_quality_main()

            if exit_code == 0:
                # Check for generated report
                report_path = PROJECT_ROOT / "reports" / "quality_report.txt"
                if report_path.exists():
                    print(f"  ✓ Quality report saved: {report_path.name}")

                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    message="All quality checks passed",
                )
            else:
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message="Some quality checks failed",
                )

        except ImportError as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=f"Could not import quality checks module: {e}",
            )
        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
