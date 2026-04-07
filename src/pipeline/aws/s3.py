"""
S3 adapter using boto3.

Provides bucket and object operations with idempotent behavior.
"""

import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class S3Adapter:
    """
    Adapter for AWS S3 operations.
    
    Provides idempotent operations for bucket and object management.
    """
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the S3 adapter.
        
        Args:
            region: AWS region.
        """
        self.region = region
        self._client = None
        self._resource = None
    
    @property
    def client(self):
        """Lazy-load the S3 client."""
        if self._client is None:
            self._client = boto3.client("s3", region_name=self.region)
        return self._client
    
    @property
    def resource(self):
        """Lazy-load the S3 resource."""
        if self._resource is None:
            self._resource = boto3.resource("s3", region_name=self.region)
        return self._resource
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.
        
        Args:
            bucket_name: Name of the bucket.
            
        Returns:
            True if bucket exists, False otherwise.
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                return False
            # 403 means bucket exists but we don't have access
            if error_code == "403":
                return True
            raise
    
    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a bucket if it doesn't exist.
        
        Args:
            bucket_name: Name of the bucket.
            
        Returns:
            True if bucket was created, False if it already existed.
        """
        if self.bucket_exists(bucket_name):
            return False
        
        # us-east-1 is special - no LocationConstraint needed
        if self.region == "us-east-1":
            self.client.create_bucket(Bucket=bucket_name)
        else:
            self.client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.region},
            )
        return True
    
    def create_folders(self, bucket_name: str, folders: list[str]) -> list[str]:
        """
        Create folder markers in a bucket.
        
        S3 doesn't have real folders, but we create empty objects
        with trailing slashes to simulate them.
        
        Args:
            bucket_name: Name of the bucket.
            folders: List of folder paths (without trailing slash).
            
        Returns:
            List of folders that were created (not already existing).
        """
        created = []
        for folder in folders:
            key = f"{folder}/" if not folder.endswith("/") else folder
            
            # Check if folder marker exists
            try:
                self.client.head_object(Bucket=bucket_name, Key=key)
                continue  # Already exists
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "404":
                    raise
            
            # Create folder marker
            self.client.put_object(Bucket=bucket_name, Key=key, Body=b"")
            created.append(folder)
        
        return created
    
    def object_exists(self, bucket_name: str, key: str) -> bool:
        """
        Check if an object exists in a bucket.
        
        Args:
            bucket_name: Name of the bucket.
            key: Object key.
            
        Returns:
            True if object exists, False otherwise.
        """
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise
    
    def list_objects(
        self, 
        bucket_name: str, 
        prefix: str = "", 
        suffix: str | None = None,
        max_keys: int = 1000,
    ) -> list[dict]:
        """
        List objects in a bucket with optional prefix/suffix filtering.
        
        Args:
            bucket_name: Name of the bucket.
            prefix: Only return objects with this prefix.
            suffix: Only return objects with this suffix (e.g., ".parquet").
            max_keys: Maximum number of keys to return.
            
        Returns:
            List of object info dicts with 'Key', 'Size', 'LastModified'.
        """
        objects = []
        paginator = self.client.get_paginator("list_objects_v2")
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys):
            for obj in page.get("Contents", []):
                if suffix and not obj["Key"].endswith(suffix):
                    continue
                objects.append({
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"],
                })
                if len(objects) >= max_keys:
                    return objects
        
        return objects
    
    def upload_file(self, local_path: Path | str, bucket_name: str, key: str) -> bool:
        """
        Upload a file to S3.
        
        Args:
            local_path: Local file path.
            bucket_name: Name of the bucket.
            key: S3 object key.
            
        Returns:
            True if upload succeeded.
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        self.client.upload_file(str(local_path), bucket_name, key)
        return True
    
    def upload_directory(
        self, 
        local_dir: Path | str, 
        bucket_name: str, 
        prefix: str,
        pattern: str = "*",
        recursive: bool = True,
    ) -> list[str]:
        """
        Upload files from a local directory to S3.
        
        Args:
            local_dir: Local directory path.
            bucket_name: Name of the bucket.
            prefix: S3 prefix for uploaded files.
            pattern: Glob pattern for files to upload.
            recursive: If True, upload files recursively.
            
        Returns:
            List of S3 keys that were uploaded.
        """
        local_dir = Path(local_dir)
        if not local_dir.exists():
            raise FileNotFoundError(f"Local directory not found: {local_dir}")
        
        uploaded = []
        glob_method = local_dir.rglob if recursive else local_dir.glob
        
        for file_path in glob_method(pattern):
            if file_path.is_file():
                # Calculate relative path for S3 key
                relative_path = file_path.relative_to(local_dir)
                key = f"{prefix}/{relative_path}".replace("\\", "/")
                
                self.upload_file(file_path, bucket_name, key)
                uploaded.append(key)
        
        return uploaded
    
    def delete_objects(self, bucket_name: str, keys: list[str]) -> int:
        """
        Delete multiple objects from a bucket.
        
        Args:
            bucket_name: Name of the bucket.
            keys: List of object keys to delete.
            
        Returns:
            Number of objects deleted.
        """
        if not keys:
            return 0
        
        # S3 delete_objects has a limit of 1000 keys per request
        deleted_count = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            response = self.client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": [{"Key": k} for k in batch]},
            )
            deleted_count += len(response.get("Deleted", []))
        
        return deleted_count
    
    def empty_bucket(self, bucket_name: str, prefix: str = "") -> int:
        """
        Delete all objects in a bucket (or with a prefix).
        
        Args:
            bucket_name: Name of the bucket.
            prefix: Only delete objects with this prefix.
            
        Returns:
            Number of objects deleted.
        """
        objects = self.list_objects(bucket_name, prefix=prefix, max_keys=10000)
        keys = [obj["Key"] for obj in objects]
        return self.delete_objects(bucket_name, keys)
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.
        
        Args:
            bucket_name: Name of the bucket.
            force: If True, empty the bucket first.
            
        Returns:
            True if bucket was deleted, False if it didn't exist.
        """
        if not self.bucket_exists(bucket_name):
            return False
        
        if force:
            self.empty_bucket(bucket_name)
        
        self.client.delete_bucket(Bucket=bucket_name)
        return True
