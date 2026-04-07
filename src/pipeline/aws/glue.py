"""
Glue adapter using boto3.

Provides database, table, job, and crawler operations with idempotent behavior
and polling for long-running operations.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

import boto3
from botocore.exceptions import ClientError


class JobRunState(Enum):
    """Glue job run states."""
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class CrawlerState(Enum):
    """Glue crawler states."""
    READY = "READY"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"


@dataclass
class JobRunResult:
    """Result of a Glue job run."""
    run_id: str
    state: JobRunState
    started_on: str | None = None
    completed_on: str | None = None
    execution_time: int = 0
    error_message: str | None = None


@dataclass
class CrawlerRunResult:
    """Result of a crawler run."""
    crawler_name: str
    state: CrawlerState
    last_crawl_status: str | None = None
    tables_created: int = 0
    tables_updated: int = 0
    error_message: str | None = None


class GlueAdapter:
    """
    Adapter for AWS Glue operations.
    
    Provides idempotent operations for databases, tables, jobs, and crawlers.
    """
    
    DEFAULT_POLL_INTERVAL = 30  # seconds for job polling
    CRAWLER_POLL_INTERVAL = 15  # seconds for crawler polling
    DEFAULT_TIMEOUT = 1800  # 30 minutes
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the Glue adapter.
        
        Args:
            region: AWS region.
        """
        self.region = region
        self._client = None
    
    @property
    def client(self):
        """Lazy-load the Glue client."""
        if self._client is None:
            self._client = boto3.client("glue", region_name=self.region)
        return self._client
    
    # ==================== Database Operations ====================
    
    def database_exists(self, database_name: str) -> bool:
        """Check if a database exists."""
        try:
            self.client.get_database(Name=database_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                return False
            raise
    
    def create_database(self, database_name: str, description: str = "") -> bool:
        """
        Create a database if it doesn't exist.
        
        Returns:
            True if database was created, False if it already existed.
        """
        if self.database_exists(database_name):
            return False
        
        self.client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": description,
            }
        )
        return True
    
    def delete_database(self, database_name: str) -> bool:
        """
        Delete a database.
        
        Returns:
            True if database was deleted, False if it didn't exist.
        """
        if not self.database_exists(database_name):
            return False
        
        self.client.delete_database(Name=database_name)
        return True
    
    # ==================== Table Operations ====================
    
    def table_exists(self, database_name: str, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.client.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                return False
            raise
    
    def create_table(
        self, 
        database_name: str, 
        table_input: dict,
    ) -> bool:
        """
        Create a table if it doesn't exist.
        
        Args:
            database_name: Name of the database.
            table_input: Table definition dict (as per Glue API).
            
        Returns:
            True if table was created, False if it already existed.
        """
        table_name = table_input.get("Name")
        if self.table_exists(database_name, table_name):
            return False
        
        self.client.create_table(
            DatabaseName=database_name,
            TableInput=table_input,
        )
        return True
    
    def create_table_from_json(
        self, 
        database_name: str, 
        json_path: str,
    ) -> bool:
        """
        Create a table from a JSON definition file.
        
        Args:
            database_name: Name of the database.
            json_path: Path to JSON file with table definition.
            
        Returns:
            True if table was created, False if it already existed.
        """
        with open(json_path, "r") as f:
            table_input = json.load(f)
        
        return self.create_table(database_name, table_input)
    
    def add_partition(
        self,
        database_name: str,
        table_name: str,
        partition_values: list[str],
        location: str,
    ) -> bool:
        """
        Add a partition to a table.
        
        Args:
            database_name: Name of the database.
            table_name: Name of the table.
            partition_values: List of partition values (in order of partition keys).
            location: S3 location for the partition data.
            
        Returns:
            True if partition was added, False if it already existed.
        """
        # Get table to retrieve storage descriptor
        try:
            table = self.client.get_table(DatabaseName=database_name, Name=table_name)
        except ClientError:
            raise ValueError(f"Table {database_name}.{table_name} not found")
        
        storage_descriptor = table["Table"]["StorageDescriptor"].copy()
        storage_descriptor["Location"] = location
        
        try:
            self.client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput={
                    "Values": partition_values,
                    "StorageDescriptor": storage_descriptor,
                },
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "AlreadyExistsException":
                return False
            raise
    
    def list_tables(self, database_name: str) -> list[str]:
        """List all tables in a database."""
        tables = []
        paginator = self.client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            for table in page.get("TableList", []):
                tables.append(table["Name"])
        return tables
    
    def delete_table(self, database_name: str, table_name: str) -> bool:
        """Delete a table."""
        if not self.table_exists(database_name, table_name):
            return False
        
        self.client.delete_table(DatabaseName=database_name, Name=table_name)
        return True
    
    # ==================== Job Operations ====================
    
    def job_exists(self, job_name: str) -> bool:
        """Check if a job exists."""
        try:
            self.client.get_job(JobName=job_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                return False
            raise
    
    def create_job(
        self,
        job_name: str,
        role_arn: str,
        script_location: str,
        description: str = "",
        glue_version: str = "4.0",
        worker_type: str = "G.1X",
        number_of_workers: int = 2,
        default_arguments: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> bool:
        """
        Create a Glue job if it doesn't exist.
        
        Args:
            job_name: Name of the job.
            role_arn: IAM role ARN for the job.
            script_location: S3 path to the job script.
            description: Job description.
            glue_version: Glue version (default "4.0").
            worker_type: Worker type (default "G.1X").
            number_of_workers: Number of workers (default 2).
            default_arguments: Default job arguments.
            timeout: Job timeout in minutes (default 60).
            
        Returns:
            True if job was created, False if it already existed.
        """
        if self.job_exists(job_name):
            return False
        
        args = {
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
        }
        if default_arguments:
            args.update(default_arguments)
        
        self.client.create_job(
            Name=job_name,
            Description=description,
            Role=role_arn,
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3",
            },
            DefaultArguments=args,
            GlueVersion=glue_version,
            WorkerType=worker_type,
            NumberOfWorkers=number_of_workers,
            Timeout=timeout,
        )
        return True
    
    def start_job_run(
        self, 
        job_name: str, 
        arguments: dict[str, str] | None = None,
    ) -> str:
        """
        Start a job run.
        
        Args:
            job_name: Name of the job.
            arguments: Optional job arguments.
            
        Returns:
            Job run ID.
        """
        kwargs: dict[str, Any] = {"JobName": job_name}
        if arguments:
            kwargs["Arguments"] = arguments
        
        response = self.client.start_job_run(**kwargs)
        return response["JobRunId"]
    
    def get_job_run_status(self, job_name: str, run_id: str) -> JobRunResult:
        """Get the status of a job run."""
        response = self.client.get_job_run(JobName=job_name, RunId=run_id)
        run = response["JobRun"]
        
        return JobRunResult(
            run_id=run_id,
            state=JobRunState(run["JobRunState"]),
            started_on=str(run.get("StartedOn", "")),
            completed_on=str(run.get("CompletedOn", "")),
            execution_time=run.get("ExecutionTime", 0),
            error_message=run.get("ErrorMessage"),
        )
    
    def wait_for_job_run(
        self,
        job_name: str,
        run_id: str,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
    ) -> JobRunResult:
        """
        Wait for a job run to complete.
        
        Args:
            job_name: Name of the job.
            run_id: Job run ID.
            poll_interval: Seconds between status checks (default 30).
            timeout: Maximum seconds to wait (default 1800).
            progress_callback: Optional callback(status: str, elapsed: int).
            
        Returns:
            Final JobRunResult.
            
        Raises:
            TimeoutError: If job doesn't complete within timeout.
        """
        poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        timeout = timeout or self.DEFAULT_TIMEOUT
        
        start_time = time.time()
        terminal_states = {
            JobRunState.SUCCEEDED,
            JobRunState.FAILED,
            JobRunState.STOPPED,
            JobRunState.TIMEOUT,
            JobRunState.ERROR,
        }
        
        while True:
            result = self.get_job_run_status(job_name, run_id)
            elapsed = int(time.time() - start_time)
            
            if progress_callback:
                progress_callback(result.state.value, elapsed)
            
            if result.state in terminal_states:
                return result
            
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Job {job_name} run {run_id} did not complete within {timeout}s"
                )
            
            time.sleep(poll_interval)
    
    def run_job_and_wait(
        self,
        job_name: str,
        arguments: dict[str, str] | None = None,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
    ) -> JobRunResult:
        """
        Start a job and wait for it to complete.
        
        Combines start_job_run and wait_for_job_run.
        """
        run_id = self.start_job_run(job_name, arguments)
        return self.wait_for_job_run(
            job_name, run_id, poll_interval, timeout, progress_callback
        )
    
    def delete_job(self, job_name: str) -> bool:
        """Delete a job."""
        if not self.job_exists(job_name):
            return False
        
        self.client.delete_job(JobName=job_name)
        return True
    
    # ==================== Crawler Operations ====================
    
    def crawler_exists(self, crawler_name: str) -> bool:
        """Check if a crawler exists."""
        try:
            self.client.get_crawler(Name=crawler_name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
                return False
            raise
    
    def get_crawler_state(self, crawler_name: str) -> CrawlerState:
        """Get the current state of a crawler."""
        response = self.client.get_crawler(Name=crawler_name)
        return CrawlerState(response["Crawler"]["State"])
    
    def create_crawler(
        self,
        crawler_name: str,
        role_arn: str,
        database_name: str,
        s3_targets: list[str],
        description: str = "",
        table_prefix: str = "",
    ) -> bool:
        """
        Create a crawler if it doesn't exist.
        
        Args:
            crawler_name: Name of the crawler.
            role_arn: IAM role ARN for the crawler.
            database_name: Target database name.
            s3_targets: List of S3 paths to crawl.
            description: Crawler description.
            table_prefix: Prefix for created tables.
            
        Returns:
            True if crawler was created, False if it already existed.
        """
        if self.crawler_exists(crawler_name):
            return False
        
        self.client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Description=description,
            Targets={
                "S3Targets": [{"Path": path} for path in s3_targets],
            },
            TablePrefix=table_prefix,
        )
        return True
    
    def start_crawler(self, crawler_name: str) -> bool:
        """
        Start a crawler.
        
        Returns:
            True if crawler was started, False if already running.
        """
        state = self.get_crawler_state(crawler_name)
        if state == CrawlerState.RUNNING:
            return False
        
        self.client.start_crawler(Name=crawler_name)
        return True
    
    def wait_for_crawler(
        self,
        crawler_name: str,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
    ) -> CrawlerRunResult:
        """
        Wait for a crawler to complete.
        
        Args:
            crawler_name: Name of the crawler.
            poll_interval: Seconds between status checks (default 15).
            timeout: Maximum seconds to wait (default 1800).
            progress_callback: Optional callback(status: str, elapsed: int).
            
        Returns:
            CrawlerRunResult with final status.
            
        Raises:
            TimeoutError: If crawler doesn't complete within timeout.
        """
        poll_interval = poll_interval or self.CRAWLER_POLL_INTERVAL
        timeout = timeout or self.DEFAULT_TIMEOUT
        
        start_time = time.time()
        
        while True:
            response = self.client.get_crawler(Name=crawler_name)
            crawler = response["Crawler"]
            state = CrawlerState(crawler["State"])
            elapsed = int(time.time() - start_time)
            
            if progress_callback:
                progress_callback(state.value, elapsed)
            
            if state == CrawlerState.READY:
                # Crawler finished
                last_crawl = crawler.get("LastCrawl", {})
                return CrawlerRunResult(
                    crawler_name=crawler_name,
                    state=state,
                    last_crawl_status=last_crawl.get("Status"),
                    tables_created=last_crawl.get("TablesCreated", 0),
                    tables_updated=last_crawl.get("TablesUpdated", 0),
                    error_message=last_crawl.get("ErrorMessage"),
                )
            
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Crawler {crawler_name} did not complete within {timeout}s"
                )
            
            time.sleep(poll_interval)
    
    def run_crawler_and_wait(
        self,
        crawler_name: str,
        poll_interval: int | None = None,
        timeout: int | None = None,
        progress_callback: Callable[[str, int], None] | None = None,
    ) -> CrawlerRunResult:
        """
        Start a crawler and wait for it to complete.
        
        If the crawler is already running, just waits for completion.
        """
        self.start_crawler(crawler_name)
        return self.wait_for_crawler(
            crawler_name, poll_interval, timeout, progress_callback
        )
    
    def delete_crawler(self, crawler_name: str) -> bool:
        """Delete a crawler."""
        if not self.crawler_exists(crawler_name):
            return False
        
        self.client.delete_crawler(Name=crawler_name)
        return True
