"""
Tests for the pipeline module.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.pipeline.cli import parse_args, PipelineArgs
from src.pipeline.steps.base import Step, StepResult, StepStatus


class TestCLI:
    """Tests for CLI argument parsing."""

    def test_default_args(self):
        """Test default argument values."""
        args = parse_args([])
        assert args.skip_to == 1
        assert args.stop_at == 19
        assert args.dry_run is False
        assert args.yes is False
        assert args.verbose is False

    def test_skip_to_arg(self):
        """Test --skip-to argument."""
        args = parse_args(["--skip-to", "5"])
        assert args.skip_to == 5

    def test_stop_at_arg(self):
        """Test --stop-at argument."""
        args = parse_args(["--stop-at", "10"])
        assert args.stop_at == 10

    def test_dry_run_arg(self):
        """Test --dry-run flag."""
        args = parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_yes_arg(self):
        """Test -y/--yes flag."""
        args = parse_args(["-y"])
        assert args.yes is True
        
        args = parse_args(["--yes"])
        assert args.yes is True

    def test_verbose_arg(self):
        """Test -v/--verbose flag."""
        args = parse_args(["-v"])
        assert args.verbose is True

    def test_combined_args(self):
        """Test combining multiple arguments."""
        args = parse_args(["--skip-to", "3", "--stop-at", "15", "--dry-run", "-y"])
        assert args.skip_to == 3
        assert args.stop_at == 15
        assert args.dry_run is True
        assert args.yes is True

    def test_invalid_skip_to_range(self):
        """Test that invalid --skip-to values are rejected."""
        with pytest.raises(SystemExit):
            parse_args(["--skip-to", "0"])
        
        with pytest.raises(SystemExit):
            parse_args(["--skip-to", "20"])

    def test_invalid_stop_at_range(self):
        """Test that invalid --stop-at values are rejected."""
        with pytest.raises(SystemExit):
            parse_args(["--stop-at", "0"])
        
        with pytest.raises(SystemExit):
            parse_args(["--stop-at", "20"])

    def test_skip_to_greater_than_stop_at(self):
        """Test that skip-to > stop-at is rejected."""
        with pytest.raises(SystemExit):
            parse_args(["--skip-to", "10", "--stop-at", "5"])


class TestStepResult:
    """Tests for StepResult class."""

    def test_success_property(self):
        """Test success property for different statuses."""
        success_result = StepResult(
            step_number=1,
            name="Test",
            status=StepStatus.SUCCESS,
            duration_seconds=1.0,
        )
        assert success_result.success is True

        skipped_result = StepResult(
            step_number=1,
            name="Test",
            status=StepStatus.SKIPPED,
            duration_seconds=0.0,
        )
        assert skipped_result.success is True

        failed_result = StepResult(
            step_number=1,
            name="Test",
            status=StepStatus.FAILED,
            duration_seconds=1.0,
        )
        assert failed_result.success is False

        error_result = StepResult(
            step_number=1,
            name="Test",
            status=StepStatus.ERROR,
            duration_seconds=1.0,
        )
        assert error_result.success is False


class TestSTSAdapter:
    """Tests for STS adapter with mocked boto3."""

    @patch("src.pipeline.aws.sts.boto3.client")
    def test_verify_credentials_success(self, mock_client):
        """Test successful credential verification."""
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {
            "Account": "123456789012",
            "Arn": "arn:aws:iam::123456789012:user/test",
            "UserId": "AIDAEXAMPLE",
        }
        mock_client.return_value = mock_sts

        from src.pipeline.aws.sts import STSAdapter
        adapter = STSAdapter(region="us-east-1")
        is_valid, message = adapter.verify_credentials()

        assert is_valid is True
        assert "123456789012" in message

    @patch("src.pipeline.aws.sts.boto3.client")
    def test_verify_credentials_failure(self, mock_client):
        """Test credential verification failure."""
        from botocore.exceptions import ClientError
        
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.side_effect = ClientError(
            {"Error": {"Code": "InvalidClientTokenId", "Message": "Invalid token"}},
            "GetCallerIdentity",
        )
        mock_client.return_value = mock_sts

        from src.pipeline.aws.sts import STSAdapter
        adapter = STSAdapter(region="us-east-1")
        is_valid, message = adapter.verify_credentials()

        assert is_valid is False
        assert "Invalid" in message or "failed" in message.lower()


class TestS3Adapter:
    """Tests for S3 adapter with mocked boto3."""

    @patch("src.pipeline.aws.s3.boto3.client")
    def test_bucket_exists_true(self, mock_client):
        """Test bucket exists check when bucket exists."""
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_client.return_value = mock_s3

        from src.pipeline.aws.s3 import S3Adapter
        adapter = S3Adapter(region="us-east-1")
        
        assert adapter.bucket_exists("my-bucket") is True

    @patch("src.pipeline.aws.s3.boto3.client")
    def test_bucket_exists_false(self, mock_client):
        """Test bucket exists check when bucket doesn't exist."""
        from botocore.exceptions import ClientError
        
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}},
            "HeadBucket",
        )
        mock_client.return_value = mock_s3

        from src.pipeline.aws.s3 import S3Adapter
        adapter = S3Adapter(region="us-east-1")
        
        assert adapter.bucket_exists("nonexistent-bucket") is False

    @patch("src.pipeline.aws.s3.boto3.client")
    def test_create_bucket_us_east_1(self, mock_client):
        """Test bucket creation in us-east-1 (no LocationConstraint)."""
        from botocore.exceptions import ClientError
        
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}},
            "HeadBucket",
        )
        mock_client.return_value = mock_s3

        from src.pipeline.aws.s3 import S3Adapter
        adapter = S3Adapter(region="us-east-1")
        
        created = adapter.create_bucket("new-bucket")
        
        assert created is True
        mock_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")


class TestGlueAdapter:
    """Tests for Glue adapter with mocked boto3."""

    @patch("src.pipeline.aws.glue.boto3.client")
    def test_database_exists_true(self, mock_client):
        """Test database exists check when database exists."""
        mock_glue = MagicMock()
        mock_glue.get_database.return_value = {"Database": {"Name": "test_db"}}
        mock_client.return_value = mock_glue

        from src.pipeline.aws.glue import GlueAdapter
        adapter = GlueAdapter(region="us-east-1")
        
        assert adapter.database_exists("test_db") is True

    @patch("src.pipeline.aws.glue.boto3.client")
    def test_database_exists_false(self, mock_client):
        """Test database exists check when database doesn't exist."""
        from botocore.exceptions import ClientError
        
        mock_glue = MagicMock()
        mock_glue.get_database.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException"}},
            "GetDatabase",
        )
        mock_client.return_value = mock_glue

        from src.pipeline.aws.glue import GlueAdapter
        adapter = GlueAdapter(region="us-east-1")
        
        assert adapter.database_exists("nonexistent_db") is False

    @patch("src.pipeline.aws.glue.boto3.client")
    def test_job_run_states(self, mock_client):
        """Test job run state handling."""
        from src.pipeline.aws.glue import JobRunState
        
        assert JobRunState.SUCCEEDED.value == "SUCCEEDED"
        assert JobRunState.FAILED.value == "FAILED"
        assert JobRunState.RUNNING.value == "RUNNING"


class TestAthenaAdapter:
    """Tests for Athena adapter with mocked boto3."""

    @patch("src.pipeline.aws.athena.boto3.client")
    def test_workgroup_exists_true(self, mock_client):
        """Test workgroup exists check when workgroup exists."""
        mock_athena = MagicMock()
        mock_athena.get_work_group.return_value = {"WorkGroup": {"Name": "test_wg"}}
        mock_client.return_value = mock_athena

        from src.pipeline.aws.athena import AthenaAdapter
        adapter = AthenaAdapter(region="us-east-1")
        
        assert adapter.workgroup_exists("test_wg") is True

    @patch("src.pipeline.aws.athena.boto3.client")
    def test_workgroup_exists_false(self, mock_client):
        """Test workgroup exists check when workgroup doesn't exist."""
        from botocore.exceptions import ClientError
        
        mock_athena = MagicMock()
        mock_athena.get_work_group.side_effect = ClientError(
            {"Error": {"Code": "InvalidRequestException"}},
            "GetWorkGroup",
        )
        mock_client.return_value = mock_athena

        from src.pipeline.aws.athena import AthenaAdapter
        adapter = AthenaAdapter(region="us-east-1")
        
        assert adapter.workgroup_exists("nonexistent_wg") is False

    @patch("src.pipeline.aws.athena.boto3.client")
    def test_query_states(self, mock_client):
        """Test query state handling."""
        from src.pipeline.aws.athena import QueryState
        
        assert QueryState.SUCCEEDED.value == "SUCCEEDED"
        assert QueryState.FAILED.value == "FAILED"
        assert QueryState.RUNNING.value == "RUNNING"


class TestPipelineRunner:
    """Tests for the pipeline runner."""

    def test_step_definitions_count(self):
        """Test that all 19 steps are defined."""
        from src.pipeline.runner import STEP_DEFINITIONS
        
        assert len(STEP_DEFINITIONS) == 19

    def test_step_definitions_numbers(self):
        """Test that step numbers are sequential 1-19."""
        from src.pipeline.runner import STEP_DEFINITIONS
        
        numbers = [s.number for s in STEP_DEFINITIONS]
        assert numbers == list(range(1, 20))

    def test_step_definitions_have_classes(self):
        """Test that all steps have step classes assigned."""
        from src.pipeline.runner import STEP_DEFINITIONS
        
        for step_def in STEP_DEFINITIONS:
            assert step_def.step_class is not None, f"Step {step_def.number} missing step_class"

    def test_run_pipeline_dry_run(self):
        """Test running pipeline in dry-run mode."""
        from src.pipeline.runner import run_pipeline
        from src.pipeline.cli import PipelineArgs
        
        args = PipelineArgs(
            skip_to=1,
            stop_at=3,
            dry_run=True,
            yes=True,
            verbose=False,
        )
        
        exit_code = run_pipeline(args)
        assert exit_code == 0
