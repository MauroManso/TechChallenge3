"""
Pipeline module entry point.

Allows running the pipeline with: python -m src.pipeline
"""

import sys
from .runner import main

if __name__ == "__main__":
    sys.exit(main())
