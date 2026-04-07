"""
Step 17: Run Athena Queries

Runs validation queries against the gold tables.
"""

import time
from datetime import datetime

from ..aws import AthenaAdapter
from ..config import ATHENA_VALIDATION_QUERIES, ATHENA_WORKGROUP, AWS_REGION, GLUE_DATABASE
from .base import Step, StepResult, StepStatus


class Step17RunAthenaQueries(Step):
    """Run Athena validation queries."""

    @property
    def number(self) -> int:
        return 17

    @property
    def name(self) -> str:
        return "run-athena-queries"

    @property
    def description(self) -> str:
        return "Run Athena queries"

    def check_can_skip(self) -> tuple[bool, str]:
        # Always run validation queries
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
            athena = AthenaAdapter(region=AWS_REGION)

            print(f"  Database: {GLUE_DATABASE}")
            print(f"  Workgroup: {ATHENA_WORKGROUP}")
            print(f"  Running {len(ATHENA_VALIDATION_QUERIES)} validation queries...")

            results = []
            failed = 0

            for name, query in ATHENA_VALIDATION_QUERIES:
                print(f"\n  [{name}]")
                print(f"  Query: {query[:60]}..." if len(query) > 60 else f"  Query: {query}")

                try:
                    result = athena.run_query_and_wait(
                        query=query,
                        database=GLUE_DATABASE,
                        workgroup=ATHENA_WORKGROUP,
                        poll_interval=2,
                        timeout=120,
                        fetch_results=True,
                    )

                    if result.state.value == "SUCCEEDED":
                        print(f"  ✓ Succeeded ({result.execution_time_ms}ms)")
                        
                        # Show results preview
                        if result.rows:
                            if result.column_names:
                                print(f"  Columns: {', '.join(result.column_names)}")
                            for row in result.rows[:3]:
                                print(f"  → {row}")
                            if len(result.rows) > 3:
                                print(f"  ... and {len(result.rows) - 3} more rows")
                        
                        results.append((name, True, None))
                    else:
                        print(f"  ✗ Failed: {result.error_message}")
                        results.append((name, False, result.error_message))
                        failed += 1

                except Exception as e:
                    print(f"  ✗ Error: {e}")
                    results.append((name, False, str(e)))
                    failed += 1

            # Summary
            print(f"\n  Summary: {len(results) - failed}/{len(results)} queries succeeded")

            if failed > 0:
                return StepResult(
                    step_number=self.number,
                    name=self.description,
                    status=StepStatus.FAILED,
                    duration_seconds=time.time() - start_time,
                    message=f"{failed} queries failed",
                )

            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                message=f"All {len(results)} queries succeeded",
            )

        except Exception as e:
            return StepResult(
                step_number=self.number,
                name=self.description,
                status=StepStatus.ERROR,
                duration_seconds=time.time() - start_time,
                message=str(e),
            )
