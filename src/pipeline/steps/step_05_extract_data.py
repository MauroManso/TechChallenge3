"""
Step 05: Extract Local Data

Extracts microdados from ZIP files to the local bronze directory.
Reuses the existing extract_microdados.py module.
"""

import time

from ..config import DATA_BRONZE_LOCAL, DATA_MICRODADOS
from .base import Step, StepResult, StepStatus


class Step05ExtractData(Step):
    """Extract microdados from ZIP files locally."""

    @property
    def number(self) -> int:
        return 5

    @property
    def name(self) -> str:
        return "extract-local-data"

    @property
    def description(self) -> str:
        return "Extract local data"

    def check_can_skip(self) -> tuple[bool, str]:
        # Check if CSV files already exist in bronze directory
        csv_files = list(DATA_BRONZE_LOCAL.rglob("*.csv"))
        if csv_files:
            return True, f"Local bronze data already extracted ({len(csv_files)} CSV files)"
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
            # Import the extraction module
            from ...data.extract_microdados import extract_microdados

            # Ensure output directory exists
            DATA_BRONZE_LOCAL.mkdir(parents=True, exist_ok=True)

            print(f"  Source: {DATA_MICRODADOS}")
            print(f"  Target: {DATA_BRONZE_LOCAL}")
            print("  Extracting months: 09, 10, 11 (2020)")

            # Run extraction
            extracted = extract_microdados(DATA_MICRODADOS, DATA_BRONZE_LOCAL)

            if extracted:
                print(f"  ✓ Extracted {len(extracted)} files")
                for f in extracted[:5]:  # Show first 5
                    print(f"    - {f.name}")
                if len(extracted) > 5:
                    print(f"    ... and {len(extracted) - 5} more")
            else:
                print("  ⚠ No files were extracted")
                print("  Check that ZIP files exist in data/microdados/dados/")

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS if extracted else StepStatus.FAILED,
                duration_seconds=time.time() - start_time,
                message=f"Extracted {len(extracted)} files" if extracted else "No files extracted",
            )

        except ImportError as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=f"Could not import extract_microdados: {e}",
            )
        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
