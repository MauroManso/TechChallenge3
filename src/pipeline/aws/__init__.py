"""
AWS service adapters using boto3.

Provides idempotent operations for STS, S3, IAM, Glue, and Athena.
"""

from .sts import STSAdapter
from .s3 import S3Adapter
from .iam import IAMAdapter
from .glue import GlueAdapter
from .athena import AthenaAdapter

__all__ = ["STSAdapter", "S3Adapter", "IAMAdapter", "GlueAdapter", "AthenaAdapter"]
