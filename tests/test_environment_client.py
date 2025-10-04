import json
import logging
from dotenv import load_dotenv
import os
import pytest

from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.http_status import HttpStatus


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings (J3)"]
__maintainer__ = "Jeffrey Jonathan Jennings (J3)"
__email__      = "j3@thej3.com"
__status__     = "dev"
 

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def environment_id():
    """Load the Test Environment ID from the environment variables."""
    load_dotenv()
    return os.getenv("TEST_ENVIRONMENT_ID")

@pytest.fixture
def kafka_cluster_id():
    """Load the Test Kafka cluster ID from the environment variables."""
    load_dotenv()
    return os.getenv("TEST_KAFKA_CLUSTER_ID")

@pytest.fixture
def principal_id():
    """Load the Test Principal ID from the environment variables."""
    load_dotenv()
    return os.getenv("PRINCIPAL_ID")
 
@pytest.fixture
def environment_client():
    """Load the Confluent Cloud credentials from the environment variables."""
    load_dotenv()
    environment_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    yield EnvironmentClient(environment_config)


class TestEnvironmentClient:
    """Test Suite for the EnvironmentClient class."""

    def test_get_environment_list(self, environment_client):
        """Test the get_environment_list() function."""

        http_status_code, error_message, environments = environment_client.get_environment_list()
    
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("Environments: %d", len(environments))

            for environment in environments:
                beautified = json.dumps(environment, indent=4, sort_keys=True)
                logger.info(beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Environments: %s", http_status_code, error_message, environments)
            return
        
    def test_get_kafka_cluster_list(self, environment_client, environment_id):
        """Test the get_kafka_cluster_list() function."""

        http_status_code, error_message, kafka_clusters = environment_client.get_kafka_cluster_list(environment_id=environment_id)
    
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("Kafka Clusters: %d", len(kafka_clusters))

            for kafka_cluster in kafka_clusters:
                beautified = json.dumps(kafka_cluster, indent=4, sort_keys=True)
                logger.info(beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Kafka Clusters: %s", http_status_code, error_message, kafka_clusters)
            return