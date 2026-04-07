"""
IAM adapter using boto3.

Provides role and policy management with idempotent behavior.
"""

import json
import time

import boto3
from botocore.exceptions import ClientError


class IAMAdapter:
    """
    Adapter for AWS IAM operations.
    
    Provides idempotent operations for role and policy management.
    """
    
    # Standard trust policy for Glue service
    GLUE_TRUST_POLICY = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the IAM adapter.
        
        Args:
            region: AWS region (IAM is global but we keep for consistency).
        """
        self.region = region
        self._client = None
    
    @property
    def client(self):
        """Lazy-load the IAM client."""
        if self._client is None:
            self._client = boto3.client("iam", region_name=self.region)
        return self._client
    
    def role_exists(self, role_name: str) -> bool:
        """
        Check if a role exists.
        
        Args:
            role_name: Name of the role.
            
        Returns:
            True if role exists, False otherwise.
        """
        try:
            self.client.get_role(RoleName=role_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
                return False
            raise
    
    def get_role_arn(self, role_name: str) -> str | None:
        """
        Get the ARN of a role.
        
        Args:
            role_name: Name of the role.
            
        Returns:
            Role ARN or None if role doesn't exist.
        """
        try:
            response = self.client.get_role(RoleName=role_name)
            return response["Role"]["Arn"]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
                return None
            raise
    
    def create_role(
        self, 
        role_name: str, 
        trust_policy: dict | None = None,
        description: str = "",
    ) -> tuple[bool, str]:
        """
        Create a role if it doesn't exist.
        
        Args:
            role_name: Name of the role.
            trust_policy: Trust policy document (defaults to Glue service).
            description: Role description.
            
        Returns:
            Tuple of (created, arn). created is True if role was created,
            False if it already existed.
        """
        if self.role_exists(role_name):
            arn = self.get_role_arn(role_name)
            return False, arn
        
        if trust_policy is None:
            trust_policy = self.GLUE_TRUST_POLICY
        
        response = self.client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=description,
        )
        
        # Wait for role to be available
        time.sleep(2)
        
        return True, response["Role"]["Arn"]
    
    def attach_policy(self, role_name: str, policy_arn: str) -> bool:
        """
        Attach a managed policy to a role.
        
        Args:
            role_name: Name of the role.
            policy_arn: ARN of the policy to attach.
            
        Returns:
            True if policy was attached, False if already attached.
        """
        # Check if already attached
        attached = self.list_attached_policies(role_name)
        if policy_arn in [p["PolicyArn"] for p in attached]:
            return False
        
        self.client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        return True
    
    def detach_policy(self, role_name: str, policy_arn: str) -> bool:
        """
        Detach a managed policy from a role.
        
        Args:
            role_name: Name of the role.
            policy_arn: ARN of the policy to detach.
            
        Returns:
            True if policy was detached, False if not attached.
        """
        try:
            self.client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
                return False
            raise
    
    def list_attached_policies(self, role_name: str) -> list[dict]:
        """
        List policies attached to a role.
        
        Args:
            role_name: Name of the role.
            
        Returns:
            List of attached policy info with 'PolicyName' and 'PolicyArn'.
        """
        try:
            response = self.client.list_attached_role_policies(RoleName=role_name)
            return response.get("AttachedPolicies", [])
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
                return []
            raise
    
    def delete_role(self, role_name: str, force: bool = False) -> bool:
        """
        Delete a role.
        
        Args:
            role_name: Name of the role.
            force: If True, detach all policies first.
            
        Returns:
            True if role was deleted, False if it didn't exist.
        """
        if not self.role_exists(role_name):
            return False
        
        if force:
            # Detach all policies
            for policy in self.list_attached_policies(role_name):
                self.detach_policy(role_name, policy["PolicyArn"])
        
        self.client.delete_role(RoleName=role_name)
        return True
    
    def create_glue_service_role(
        self, 
        role_name: str,
        s3_bucket: str | None = None,
    ) -> tuple[bool, str]:
        """
        Create a role suitable for Glue jobs with standard policies.
        
        Args:
            role_name: Name of the role.
            s3_bucket: Optional bucket name for S3 access (grants full S3 if None).
            
        Returns:
            Tuple of (created, arn).
        """
        created, arn = self.create_role(
            role_name,
            trust_policy=self.GLUE_TRUST_POLICY,
            description="Glue service role for Tech Challenge 3 pipeline",
        )
        
        # Attach standard Glue service policy
        self.attach_policy(
            role_name, 
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        
        # Attach S3 access (full access for simplicity, could be scoped to bucket)
        self.attach_policy(
            role_name,
            "arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
        
        return created, arn
