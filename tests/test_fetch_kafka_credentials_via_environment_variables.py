import pytest
import json
from unittest.mock import patch
from src.thread_safe_tool import _fetch_kafka_credentials_via_environment_variables


@pytest.fixture
def mock_kafka_credentials():
    """Fixture for mock Kafka credentials."""
    return [
        {
            "bootstrap.servers": "pkc-1.us-east-1.aws.confluent.cloud:9092",
            "sasl.username": "api_key_1",
            "sasl.password": "api_secret_1",
            "kafka_cluster_id": "lkc-1"
        },
        {
            "bootstrap.servers": "pkc-2.us-west-2.aws.confluent.cloud:9092",
            "sasl.username": "api_key_2",
            "sasl.password": "api_secret_2",
            "kafka_cluster_id": "lkc-2"
        }
    ]


@pytest.fixture
def mock_secrets_paths():
    """Fixture for mock AWS Secrets Manager paths."""
    return [
        {
            "region_name": "us-east-1",
            "secret_name": "kafka/cluster1"
        },
        {
            "region_name": "us-west-2",
            "secret_name": "kafka/cluster2"
        }
    ]


class TestFetchKafkaCredentialsViaEnvironmentVariables:
    """Test suite for _fetch_kafka_credentials_via_environment_variables function."""

    @patch('src.thread_safe_tool.os.getenv')
    def test_fetch_from_env_variable_success(self, mock_getenv, mock_kafka_credentials):
        """Test successful retrieval from environment variable."""
        mock_getenv.return_value = json.dumps(mock_kafka_credentials)
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False
        )
        
        assert len(result) == 2
        assert result[0]["kafka_cluster_id"] == "lkc-1"
        assert result[1]["kafka_cluster_id"] == "lkc-2"
        mock_getenv.assert_called_once_with("KAFKA_CREDENTIALS", "[]")

    @patch('src.thread_safe_tool.os.getenv')
    def test_fetch_from_env_variable_empty(self, mock_getenv):
        """Test when environment variable is empty."""
        mock_getenv.return_value = "[]"
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False
        )
        
        assert result == []

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_fetch_from_aws_secrets_manager_success(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths,
        mock_kafka_credentials
    ):
        """Test successful retrieval from AWS Secrets Manager."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.side_effect = [
            (mock_kafka_credentials[0], None),
            (mock_kafka_credentials[1], None)
        ]
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        assert len(result) == 2
        assert result[0]["bootstrap.servers"] == mock_kafka_credentials[0]["bootstrap.servers"]
        assert result[1]["sasl.username"] == mock_kafka_credentials[1]["sasl.username"]
        
        # Verify get_secrets was called with correct parameters
        assert mock_get_secrets.call_count == 2
        mock_get_secrets.assert_any_call("us-east-1", "kafka/cluster1")
        mock_get_secrets.assert_any_call("us-west-2", "kafka/cluster2")

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_fetch_from_aws_secrets_manager_failure(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths
    ):
        """Test AWS Secrets Manager retrieval failure."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.return_value = ({}, "Access denied")
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        assert result == []

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_fetch_from_aws_secrets_manager_partial_failure(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths,
        mock_kafka_credentials
    ):
        """Test AWS Secrets Manager with partial failure (first succeeds, second fails)."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.side_effect = [
            (mock_kafka_credentials[0], None),
            ({}, "Secret not found")
        ]
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        # Should return empty list on any failure
        assert result == []

    @patch('src.thread_safe_tool.os.getenv')
    def test_filter_single_cluster_from_env(self, mock_getenv, mock_kafka_credentials):
        """Test filtering a single Kafka cluster from environment variable."""
        mock_getenv.return_value = json.dumps(mock_kafka_credentials)
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False,
            kafka_cluster_filter="lkc-1"
        )
        
        assert len(result) == 1
        assert result[0]["kafka_cluster_id"] == "lkc-1"

    @patch('src.thread_safe_tool.os.getenv')
    def test_filter_multiple_clusters_from_env(self, mock_getenv, mock_kafka_credentials):
        """Test filtering multiple Kafka clusters with comma-separated values."""
        mock_getenv.return_value = json.dumps(mock_kafka_credentials)
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False,
            kafka_cluster_filter="lkc-1, lkc-2"
        )
        
        assert len(result) == 2
        cluster_ids = [cred["kafka_cluster_id"] for cred in result]
        assert "lkc-1" in cluster_ids
        assert "lkc-2" in cluster_ids

    @patch('src.thread_safe_tool.os.getenv')
    def test_filter_non_existent_cluster(self, mock_getenv, mock_kafka_credentials):
        """Test filtering with non-existent cluster ID."""
        mock_getenv.return_value = json.dumps(mock_kafka_credentials)
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False,
            kafka_cluster_filter="lkc-999"
        )
        
        assert result == []

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_filter_clusters_from_aws_secrets_manager(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths,
        mock_kafka_credentials
    ):
        """Test filtering Kafka clusters from AWS Secrets Manager."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.side_effect = [
            (mock_kafka_credentials[0], None),
            (mock_kafka_credentials[1], None)
        ]
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True,
            kafka_cluster_filter="lkc-2"
        )
        
        assert len(result) == 1
        assert result[0]["kafka_cluster_id"] == "lkc-2"

    @patch('src.thread_safe_tool.os.getenv')
    def test_exception_handling_invalid_json(self, mock_getenv):
        """Test exception handling when environment variable contains invalid JSON."""
        mock_getenv.return_value = "invalid json"
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False
        )
        
        assert result == []

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_exception_handling_get_secrets_raises(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths
    ):
        """Test exception handling when get_secrets raises an exception."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.side_effect = Exception("Connection error")
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        assert result == []

    @patch('src.thread_safe_tool.os.getenv')
    def test_empty_secrets_paths(self, mock_getenv):
        """Test when KAFKA_API_SECRET_PATHS is empty."""
        mock_getenv.return_value = "[]"
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        assert result == []

    @patch('src.thread_safe_tool.os.getenv')
    def test_missing_env_variable_uses_default(self, mock_getenv):
        """Test that missing environment variable uses default empty list."""
        mock_getenv.return_value = "[]"  # Default value
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False
        )
        
        assert result == []
        mock_getenv.assert_called_once_with("KAFKA_CREDENTIALS", "[]")

    @patch('src.thread_safe_tool.os.getenv')
    def test_filter_with_whitespace(self, mock_getenv, mock_kafka_credentials):
        """Test that filter handles whitespace correctly."""
        mock_getenv.return_value = json.dumps(mock_kafka_credentials)
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=False,
            kafka_cluster_filter="  lkc-1  ,  lkc-2  "
        )
        
        assert len(result) == 2

    @patch('src.thread_safe_tool.get_secrets')
    @patch('src.thread_safe_tool.os.getenv')
    def test_aws_secrets_manager_with_no_cluster_id(
        self, 
        mock_getenv, 
        mock_get_secrets,
        mock_secrets_paths
    ):
        """Test AWS Secrets Manager when returned secrets don't have cluster_id."""
        mock_getenv.return_value = json.dumps(mock_secrets_paths)
        mock_get_secrets.return_value = (
            {
                "bootstrap.servers": "server.com:9092",
                "sasl.username": "user",
                "sasl.password": "pass"
                # Missing kafka_cluster_id
            }, 
            None
        )
        
        result = _fetch_kafka_credentials_via_environment_variables(
            use_aws_secrets_manager=True
        )
        
        assert len(result) == 2
        assert result[0]["kafka_cluster_id"] is None