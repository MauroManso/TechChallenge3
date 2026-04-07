"""
CLI argument parsing for the pipeline runner.

Provides the same controls as the original run-all.ps1:
- --skip-to: Start from a specific step
- --stop-at: Stop at a specific step  
- --dry-run: Show what would be done without executing
"""

import argparse
import sys
from dataclasses import dataclass


@dataclass
class PipelineArgs:
    """Parsed command-line arguments for the pipeline runner."""
    skip_to: int
    stop_at: int
    dry_run: bool
    yes: bool  # Skip confirmation prompt
    verbose: bool


def parse_args(args: list[str] | None = None) -> PipelineArgs:
    """
    Parse command-line arguments.
    
    Args:
        args: List of arguments (defaults to sys.argv[1:])
        
    Returns:
        PipelineArgs with parsed values.
    """
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="PNAD COVID-19 Tech Challenge 3 Pipeline Runner",
        epilog="Example: python -m src.pipeline --skip-to 5 --stop-at 10",
    )
    
    parser.add_argument(
        "--skip-to",
        type=int,
        default=1,
        metavar="N",
        help="Start from step N (default: 1)",
    )
    
    parser.add_argument(
        "--stop-at",
        type=int,
        default=19,
        metavar="N",
        help="Stop at step N (default: 19)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    
    parsed = parser.parse_args(args)
    
    # Validate ranges
    if parsed.skip_to < 1 or parsed.skip_to > 19:
        parser.error(f"--skip-to must be between 1 and 19, got {parsed.skip_to}")
    if parsed.stop_at < 1 or parsed.stop_at > 19:
        parser.error(f"--stop-at must be between 1 and 19, got {parsed.stop_at}")
    if parsed.skip_to > parsed.stop_at:
        parser.error(f"--skip-to ({parsed.skip_to}) cannot be greater than --stop-at ({parsed.stop_at})")
    
    return PipelineArgs(
        skip_to=parsed.skip_to,
        stop_at=parsed.stop_at,
        dry_run=parsed.dry_run,
        yes=parsed.yes,
        verbose=parsed.verbose,
    )


def print_banner() -> None:
    """Print the pipeline banner."""
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║      PNAD COVID-19 - Tech Challenge 3 - Pipeline Runner      ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()


def print_step_banner(step_number: int, description: str) -> None:
    """Print a step banner."""
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print(f"║ STEP {step_number:02d}: {description:<49}║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()


def confirm_execution(skip_to: int, stop_at: int, steps_count: int) -> bool:
    """
    Prompt for confirmation before executing.
    
    Returns:
        True if user confirms, False otherwise.
    """
    try:
        response = input("Continue? (Y/n) ").strip().lower()
        return response != "n"
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return False
