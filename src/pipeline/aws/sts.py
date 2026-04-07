"""
STS (Security Token Service) adapter using boto3.

Provides credential verification operations.
"""

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dataclasses import dataclass


@dataclass
class CallerIdentity:
    """AWS caller identity information."""
    account_id: str
    arn: str
    user_id: str


class STSAdapter:
    """
    Adapter for AWS STS operations.
    
    Used primarily to verify AWS credentials are configured correctly.
    """
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the STS adapter.
        
        Args:
            region: AWS region.
        """
        self.region = region
        self._client = None
    
    @property
    def client(self):
        """Lazy-load the STS client."""
        if self._client is None:
            self._client = boto3.client("sts", region_name=self.region)
        return self._client
    
    def get_caller_identity(self) -> CallerIdentity:
        """
        Get the identity of the current AWS caller.
        
        Returns:
            CallerIdentity with account, ARN, and user ID.
            
        Raises:
            RuntimeError: If credentials are invalid or not configured.
        """
        try:
            response = self.client.get_caller_identity()
            return CallerIdentity(
                account_id=response["Account"],
                arn=response["Arn"],
                user_id=response["UserId"],
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            raise RuntimeError(
                f"AWS credential verification failed ({error_code}): {error_msg}"
            ) from e
        except BotoCoreError as e:
            raise RuntimeError(f"AWS credential verification failed: {e}") from e
    
    def verify_credentials(self) -> tuple[bool, str]:
        """
        Verify that AWS credentials are configured and valid.
        
        Returns:
            Tuple of (is_valid, message). If valid, message contains
            the account ID. If invalid, message contains the error.
        """
        try:
            identity = self.get_caller_identity()
            return True, f"Authenticated as {identity.arn} (Account: {identity.account_id})"
        except RuntimeError as e:
            return False, str(e)
