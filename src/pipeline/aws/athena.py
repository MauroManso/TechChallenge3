"""
Athena adapter using boto3.

Provides workgroup and query execution with polling.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

import boto3
from botocore.exceptions import ClientError


class QueryState(Enum):
    """Athena query execution states."""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class QueryResult:
    """Result of an Athena query execution."""
    execution_id: str
    state: QueryState
    output_location: str | None = None
    data_scanned_bytes: int = 0
    execution_time_ms: int = 0
    error_message: str | None = None
    rows: list[list[str]] | None = None
    column_names: list[str] | None = None


class AthenaAdapter:
    """
    Adapter for AWS Athena operations.
    
    Provides workgroup management and query execution with polling.
    """
    
    DEFAULT_POLL_INTERVAL = 2  # seconds
    DEFAULT_TIMEOUT = 300  # 5 minutes
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the Athena adapter.
        
        Args:
            region: AWS region.
        """
        self.region = region
        self._client = None
    
    @property
    def client(self):
        """Lazy-load the Athena client."""
        if self._client is None:
            self._client = boto3.client("athena", region_name=self.region)
        return self._client
    
    # ==================== Workgroup Operations ====================
    
    def workgroup_exists(self, workgroup_name: str) -> bool:
        """Check if a workgroup exists."""
        try:
            self.client.get_work_group(WorkGroup=workgroup_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "InvalidRequestException":
                # Workgroup doesn't exist
                return False
            raise
    
    def create_workgroup(
        self,
        workgroup_name: str,
        output_location: str,
        description: str = "",
        enforce_configuration: bool = True,
        publish_cloudwatch_metrics: bool = True,
    ) -> bool:
        """
        Create a workgroup if it doesn't exist.
        
        Args:
            workgroup_name: Name of the workgroup.
            output_location: S3 location for query results.
            description: Workgroup description.
            enforce_configuration: If True, clients can't override settings.
            publish_cloudwatch_metrics: If True, publish metrics to CloudWatch.
            
        Returns:
            True if workgroup was created, False if it already existed.
        """
        if self.workgroup_exists(workgroup_name):
            return False
        
        self.client.create_work_group(
            Name=workgroup_name,
            Description=description,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": output_location,
                },
                "EnforceWorkGroupConfiguration": enforce_configuration,
                "PublishCloudWatchMetricsEnabled": publish_cloudwatch_metrics,
            },
        )
        return True
    
    def delete_workgroup(self, workgroup_name: str, force: bool = False) -> bool:
        """
        Delete a workgroup.
        
        Args:
            workgroup_name: Name of the workgroup.
            force: If True, delete even if workgroup has saved queries.
            
        Returns:
            True if workgroup was deleted, False if it didn't exist.
        """
        if not self.workgroup_exists(workgroup_name):
            return False
        
        self.client.delete_work_group(
            WorkGroup=workgroup_name,
            RecursiveDeleteOption=force,
        )
        return True
    
    # ==================== Query Operations ====================
    
    def start_query(
        self,
        query: str,
        database: str,
        workgroup: str,
        output_location: str | None = None,
    ) -> str:
        """
        Start a query execution.
        
        Args:
            query: SQL query string.
            database: Database name.
            workgroup: Workgroup name.
            output_location: Optional output location (overrides workgroup default).
            
        Returns:
            Query execution ID.
        """
        kwargs: dict[str, Any] = {
            "QueryString": query,
            "WorkGroup": workgroup,
            "QueryExecutionContext": {"Database": database},
        }
        
        if output_location:
            kwargs["ResultConfiguration"] = {"OutputLocation": output_location}
        
        response = self.client.start_query_execution(**kwargs)
        return response["QueryExecutionId"]
    
    def get_query_status(self, execution_id: str) -> QueryResult:
        """Get the status of a query execution."""
        response = self.client.get_query_execution(QueryExecutionId=execution_id)
        execution = response["QueryExecution"]
        status = execution["Status"]
        
        result = QueryResult(
            execution_id=execution_id,
            state=QueryState(status["State"]),
            output_location=execution.get("ResultConfiguration", {}).get("OutputLocation"),
        )
        
        if "Statistics" in execution:
            stats = execution["Statistics"]
            result.data_scanned_bytes = stats.get("DataScannedInBytes", 0)
            result.execution_time_ms = stats.get("TotalExecutionTimeInMillis", 0)
        
        if status.get("StateChangeReason"):
            result.error_message = status["StateChangeReason"]
        
        return result
    
    def wait_for_query(
        self,
        execution_id: str,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
    ) -> QueryResult:
        """
        Wait for a query to complete.
        
        Args:
            execution_id: Query execution ID.
            poll_interval: Seconds between status checks (default 2).
            timeout: Maximum seconds to wait (default 300).
            progress_callback: Optional callback(status: str, elapsed: int).
            
        Returns:
            Final QueryResult.
            
        Raises:
            TimeoutError: If query doesn't complete within timeout.
        """
        poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        timeout = timeout or self.DEFAULT_TIMEOUT
        
        start_time = time.time()
        terminal_states = {QueryState.SUCCEEDED, QueryState.FAILED, QueryState.CANCELLED}
        
        while True:
            result = self.get_query_status(execution_id)
            elapsed = int(time.time() - start_time)
            
            if progress_callback:
                progress_callback(result.state.value, elapsed)
            
            if result.state in terminal_states:
                return result
            
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Query {execution_id} did not complete within {timeout}s"
                )
            
            time.sleep(poll_interval)
    
    def get_query_results(
        self,
        execution_id: str,
        max_results: int = 1000,
    ) -> QueryResult:
        """
        Get the results of a completed query.
        
        Args:
            execution_id: Query execution ID.
            max_results: Maximum number of rows to return.
            
        Returns:
            QueryResult with rows and column_names populated.
        """
        # First get the status
        result = self.get_query_status(execution_id)
        
        if result.state != QueryState.SUCCEEDED:
            return result
        
        # Get results
        response = self.client.get_query_results(
            QueryExecutionId=execution_id,
            MaxResults=max_results,
        )
        
        rows = response.get("ResultSet", {}).get("Rows", [])
        
        # First row is headers
        if rows:
            result.column_names = [
                col.get("VarCharValue", "") 
                for col in rows[0].get("Data", [])
            ]
            result.rows = [
                [col.get("VarCharValue", "") for col in row.get("Data", [])]
                for row in rows[1:]
            ]
        else:
            result.column_names = []
            result.rows = []
        
        return result
    
    def run_query_and_wait(
        self,
        query: str,
        database: str,
        workgroup: str,
        output_location: str | None = None,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
        fetch_results: bool = True,
    ) -> QueryResult:
        """
        Execute a query and wait for it to complete.
        
        Combines start_query, wait_for_query, and optionally get_query_results.
        
        Args:
            query: SQL query string.
            database: Database name.
            workgroup: Workgroup name.
            output_location: Optional output location.
            poll_interval: Seconds between status checks.
            timeout: Maximum seconds to wait.
            progress_callback: Optional progress callback.
            fetch_results: If True, fetch result rows on success.
            
        Returns:
            QueryResult with execution details and optionally rows.
        """
        execution_id = self.start_query(query, database, workgroup, output_location)
        result = self.wait_for_query(
            execution_id, poll_interval, timeout, progress_callback
        )
        
        if fetch_results and result.state == QueryState.SUCCEEDED:
            result = self.get_query_results(execution_id)
        
        return result
