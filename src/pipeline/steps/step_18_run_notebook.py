"""
Step 18: Run EDA Notebook

Executes the EDA notebook to generate visualizations and reports.
"""

import subprocess
import sys
import time
from pathlib import Path

from ..config import AWS_REGION, PROJECT_ROOT
from .base import Step, StepResult, StepStatus


class Step18RunNotebook(Step):
    """Run EDA notebook for analysis and visualizations."""

    @property
    def number(self) -> int:
        return 18

    @property
    def name(self) -> str:
        return "run-eda-notebook"

    @property
    def description(self) -> str:
        return "Run EDA notebook"

    def check_can_skip(self) -> tuple[bool, str]:
        # Always regenerate for consistency
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
            notebooks_dir = PROJECT_ROOT / "notebooks"
            reports_dir = PROJECT_ROOT / "reports"
            
            # Find notebook or py script
            eda_script = notebooks_dir / "01_eda_pnad_covid.py"
            eda_notebook = notebooks_dir / "01_eda_pnad_covid.ipynb"

            if not eda_script.exists() and not eda_notebook.exists():
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message="EDA notebook/script not found in notebooks/",
                )

            # Ensure reports dir exists
            reports_dir.mkdir(exist_ok=True)

            print(f"  Notebooks dir: {notebooks_dir}")
            print(f"  Reports dir: {reports_dir}")

            # Convert .py to .ipynb if needed
            if eda_script.exists():
                print("  Converting .py to .ipynb with jupytext...")
                result = subprocess.run(
                    [sys.executable, "-m", "jupytext", "--to", "notebook", str(eda_script)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    print(f"  ⚠ jupytext conversion warning: {result.stderr}")

            # Execute notebook
            if eda_notebook.exists():
                print("  Executing notebook with nbconvert...")
                print("  (This may take several minutes)")

                result = subprocess.run(
                    [
                        sys.executable, "-m", "jupyter", "nbconvert",
                        "--to", "notebook",
                        "--execute",
                        str(eda_notebook),
                        "--inplace",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=600,  # 10 minute timeout
                )

                if result.returncode == 0:
                    print(f"  ✓ Notebook executed successfully")
                else:
                    # Check for auth errors
                    if any(x in result.stderr for x in 
                           ["LoginRefreshRequired", "TOKEN_EXPIRED", "AccessDeniedException"]):
                        print(f"  ✗ AWS authentication error detected")
                        print(f"  Run: aws sso login")
                        return StepResult(
                            step_number=self.number,
                            name=self.description,
                            status=StepStatus.FAILED,
                            duration_seconds=time.time() - start_time,
                            message="AWS authentication expired",
                        )
                    
                    print(f"  ✗ Notebook execution failed")
                    print(f"  stderr: {result.stderr[:500]}")
                    return StepResult(
                        step_number=self.number,
                        name=self.description,
                        status=StepStatus.FAILED,
                        duration_seconds=time.time() - start_time,
                        message=f"Notebook execution failed: {result.stderr[:100]}",
                    )

            # Check generated reports
            png_files = list(reports_dir.glob("*.png"))
            print(f"  Generated PNGs: {len(png_files)}")
            for f in png_files[:5]:
                print(f"    - {f.name}")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"Notebook complete, {len(png_files)} PNGs generated",
            )

        except subprocess.TimeoutExpired:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.FAILED,
                duration_seconds=time.time() - start_time,
                message="Notebook execution timed out (10 min)",
            )
        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
