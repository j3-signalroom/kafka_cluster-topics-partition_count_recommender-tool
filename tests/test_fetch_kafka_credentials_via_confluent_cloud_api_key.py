import json
import pytest
from unittest.mock import Mock, patch
import logging

from src.thread_safe_tool import _fetch_kafka_credentials_via_confluent_cloud_api_key, HttpStatus


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def principal_id():
    """Fixture for principal ID."""
    return "sa-12345"


@pytest.fixture
def environment_config():
    """Fixture for environment configuration."""
    return {
        "api_key": "test_key",
        "api_secret": "test_secret"
    }


@pytest.fixture
def mock_environments():
    """Fixture for mock environment data."""
    return [
        {"id": "env-1", "name": "dev"},
        {"id": "env-2", "name": "prod"}
    ]


@pytest.fixture
def mock_kafka_clusters():
    """Fixture for mock Kafka cluster data."""
    return [
        {
            "id": "lkc-1",
            "kafka_bootstrap_endpoint": "pkc-1.us-east-1.aws.confluent.cloud:9092",
            "cloud_provider": "aws",
            "region_name": "us-east-1"
        },
        {
            "id": "lkc-2",
            "kafka_bootstrap_endpoint": "pkc-2.us-west-2.aws.confluent.cloud:9092",
            "cloud_provider": "aws",
            "region_name": "us-west-2"
        }
    ]


@pytest.fixture
def mock_api_key_pair():
    """Fixture for mock API key pair."""
    return {
        "key": "test_api_key",
        "secret": "test_api_secret"
    }


@pytest.fixture
def mock_environment_client():
    """Fixture for mock EnvironmentClient."""
    with patch('src.thread_safe_tool.EnvironmentClient') as mock_client:
        yield mock_client


class TestFetchKafkaCredentialsViaConfluentCloudApiKey:
    """Test suite for _fetch_kafka_credentials_via_confluent_cloud_api_key function."""

    def test_successful_fetch_all_environments(
        self, 
        principal_id, 
        environment_config, 
        mock_environments, 
        mock_kafka_clusters,
        mock_api_key_pair,
        mock_environment_client
    ):
        """Test successful retrieval of Kafka credentials from all environments."""
        # Setup mock client
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, mock_environments
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, [mock_kafka_clusters[0]]
        )
        mock_client_instance.create_kafka_api_key.return_value = (
            HttpStatus.ACCEPTED, None, mock_api_key_pair
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert
        assert len(result) == 2  # 2 environments
        assert result[0]["bootstrap.servers"] == mock_kafka_clusters[0]["kafka_bootstrap_endpoint"]
        assert result[0]["sasl.username"] == mock_api_key_pair["key"]
        assert result[0]["sasl.password"] == mock_api_key_pair["secret"]
        assert result[0]["kafka_cluster_id"] == mock_kafka_clusters[0]["id"]

        beautified = json.dumps(mock_api_key_pair, indent=4, sort_keys=True)
        logger.info("Test successful_fetch_all_environments passed with the following results: \n%s", beautified)

    def test_environment_list_failure(
        self, 
        principal_id, 
        environment_config,
        mock_environment_client
    ):
        """Test handling of environment list retrieval failure."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.INTERNAL_SERVER_ERROR, "API Error", []
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert
        assert result == []

    def test_kafka_cluster_list_failure(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_environment_client
    ):
        """Test handling of Kafka cluster list retrieval failure."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, mock_environments
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.INTERNAL_SERVER_ERROR, "Cluster API Error", []
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert
        assert result == []

    def test_environment_filter(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_api_key_pair,
        mock_environment_client
    ):
        """Test filtering by specific environment IDs."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, mock_environments
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, [mock_kafka_clusters[0]]
        )
        mock_client_instance.create_kafka_api_key.return_value = (
            HttpStatus.ACCEPTED, None, mock_api_key_pair
        )
        
        # Execute with environment filter
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, 
            environment_config,
            environment_filter="env-1"
        )
        
        # Assert - only one environment should be processed
        assert len(result) == 1
        mock_client_instance.get_kafka_cluster_list.assert_called_once_with(
            environment_id="env-1"
        )

    def test_kafka_cluster_filter(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_api_key_pair,
        mock_environment_client
    ):
        """Test filtering by specific Kafka cluster IDs."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, [mock_environments[0]]
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, mock_kafka_clusters
        )
        mock_client_instance.create_kafka_api_key.return_value = (
            HttpStatus.ACCEPTED, None, mock_api_key_pair
        )
        
        # Execute with Kafka cluster filter
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, 
            environment_config,
            kafka_cluster_filter="lkc-1"
        )
        
        # Assert - only one cluster should be processed
        assert len(result) == 1
        assert result[0]["kafka_cluster_id"] == "lkc-1"

    def test_forbidden_access(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_environment_client
    ):
        """Test handling of forbidden access (403) error."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, [mock_environments[0]]
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, [mock_kafka_clusters[0]]
        )
        mock_client_instance.create_kafka_api_key.return_value = (
            HttpStatus.FORBIDDEN, "Access denied", None
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert - should continue without error but return empty credentials
        assert result == []

    def test_api_key_creation_failure_returns_empty(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_environment_client
    ):
        """Test API key creation failure returns empty list and exits early."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, [mock_environments[0]]
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, mock_kafka_clusters
        )
        
        # First cluster succeeds, second cluster fails
        mock_client_instance.create_kafka_api_key.side_effect = [
            (HttpStatus.ACCEPTED, None, {"key": "key1", "secret": "secret1"}),
            (HttpStatus.INTERNAL_SERVER_ERROR, "API Error", None)
        ]
        mock_client_instance.delete_kafka_api_key.return_value = (
            HttpStatus.ACCEPTED, None
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert - should return empty list when any cluster fails
        # The cleanup logic would only trigger if the SAME cluster had multiple keys,
        # which doesn't happen in this single-pass loop
        assert result == []
        # No deletion occurs because the failed cluster (lkc-2) has no existing credentials yet
        mock_client_instance.delete_kafka_api_key.assert_not_called()

    def test_cleanup_deletion_failure(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_api_key_pair,
        mock_environment_client
    ):
        """Test handling when cleanup deletion fails."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, [mock_environments[0]]
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, mock_kafka_clusters
        )
        
        # First cluster succeeds, second fails
        mock_client_instance.create_kafka_api_key.side_effect = [
            (HttpStatus.ACCEPTED, None, mock_api_key_pair),
            (HttpStatus.INTERNAL_SERVER_ERROR, "API Error", None)
        ]
        # Deletion fails
        mock_client_instance.delete_kafka_api_key.return_value = (
            HttpStatus.INTERNAL_SERVER_ERROR, "Delete failed"
        )
        
        # Execute - should not raise exception despite cleanup failure
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert
        assert result == []

    def test_multiple_environment_ids_filter(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_kafka_clusters,
        mock_api_key_pair,
        mock_environment_client
    ):
        """Test filtering with multiple comma-separated environment IDs."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, mock_environments
        )
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, [mock_kafka_clusters[0]]
        )
        mock_client_instance.create_kafka_api_key.return_value = (
            HttpStatus.ACCEPTED, None, mock_api_key_pair
        )
        
        # Execute with multiple environment filters
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, 
            environment_config,
            environment_filter="env-1, env-2"
        )
        
        # Assert - both environments should be processed
        assert len(result) == 2

    def test_no_kafka_credentials_found(
        self, 
        principal_id, 
        environment_config,
        mock_environments,
        mock_environment_client
    ):
        """Test when no Kafka credentials are found."""
        mock_client_instance = Mock()
        mock_environment_client.return_value = mock_client_instance
        
        mock_client_instance.get_environment_list.return_value = (
            HttpStatus.OK, None, mock_environments
        )
        # No Kafka clusters in environments
        mock_client_instance.get_kafka_cluster_list.return_value = (
            HttpStatus.OK, None, []
        )
        
        # Execute
        result = _fetch_kafka_credentials_via_confluent_cloud_api_key(
            principal_id, environment_config
        )
        
        # Assert
        assert result == []