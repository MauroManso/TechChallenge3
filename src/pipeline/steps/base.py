"""
Base step class for pipeline steps.

All pipeline steps inherit from this class and implement the run() method.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


class StepStatus(Enum):
    """Status of a pipeline step execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    ERROR = "error"


@dataclass
class StepResult:
    """Result of a pipeline step execution."""
    step_number: int
    name: str
    status: StepStatus
    duration_seconds: float
    message: str = ""
    details: dict[str, Any] | None = None

    @property
    def success(self) -> bool:
        """Return True if step succeeded or was skipped."""
        return self.status in (StepStatus.SUCCESS, StepStatus.SKIPPED)


class Step(ABC):
    """
    Abstract base class for pipeline steps.
    
    Each step must implement:
    - number: The step number (1-19)
    - name: Human-readable step name
    - description: Brief description
    - run(): The actual step logic
    - check_can_skip(): Optional idempotency check
    """

    @property
    @abstractmethod
    def number(self) -> int:
        """Step number (1-19)."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable step name."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Brief description of what this step does."""
        pass

    def check_can_skip(self) -> tuple[bool, str]:
        """
        Check if this step can be skipped (idempotency check).
        
        Returns:
            Tuple of (can_skip, reason). If can_skip is True, the step
            will be skipped with the given reason message.
        """
        return False, ""

    @abstractmethod
    def run(self, dry_run: bool = False) -> StepResult:
        """
        Execute the step.
        
        Args:
            dry_run: If True, only log what would be done without executing.
            
        Returns:
            StepResult with execution outcome.
        """
        pass

    def __repr__(self) -> str:
        return f"Step({self.number:02d}: {self.name})"
