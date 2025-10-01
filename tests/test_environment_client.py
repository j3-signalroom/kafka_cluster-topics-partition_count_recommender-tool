import json
import logging
import time
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

    def test_create_kafka_api_key(self, environment_client, kafka_cluster_id, principal_id):
        """Test the create_kafka_api_key() function."""

        http_status_code, error_message, api_key_pair = environment_client.create_kafka_api_key(kafka_cluster_id=kafka_cluster_id, principal_id=principal_id)
    
        try:
            assert http_status_code == HttpStatus.ACCEPTED, f"HTTP Status Code: {http_status_code}"

            beautified = json.dumps(api_key_pair, indent=4, sort_keys=True)
            logger.info("Kafka API Key Pair: %s", beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Kafka API Key Pair: %s", http_status_code, error_message, api_key_pair)
            return
        
    def test_delete_kafka_api_key(self, environment_client, kafka_cluster_id, principal_id):
        """Test the delete_kafka_api_key() function."""

        http_status_code, error_message, api_key_pair = environment_client.create_kafka_api_key(kafka_cluster_id=kafka_cluster_id, principal_id=principal_id)
    
        try:
            assert http_status_code == HttpStatus.ACCEPTED, f"HTTP Status Code: {http_status_code}"

            beautified = json.dumps(api_key_pair, indent=4, sort_keys=True)
            logger.info("Kafka API Key Pair: %s", beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Kafka API Key Pair: %s", http_status_code, error_message, api_key_pair)
            return

        time.sleep(2)  # Wait for 2 seconds before deleting the API key.

        http_status_code, error_message = environment_client.delete_kafka_api_key(api_key=api_key_pair["key"])
    
        try:
            assert http_status_code == HttpStatus.NO_CONTENT, f"HTTP Status Code: {http_status_code}"

            logger.info("Successfully deleted Kafka API Key: %s", api_key_pair['key'])
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s", http_status_code, error_message)
            return  

    def test_iterating_through_environments_creating_and_deleting_kafka_api_keys(self, environment_client, principal_id):
        """Test the create_kafka_api_key() and delete_kafka_api_key() functions."""

        environments_with_kafka_clusters = {}

        http_status_code, error_message, environments = environment_client.get_environment_list()
    
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("Environments: %d", len(environments))

            for environment in environments:
                http_status_code, error_message, kafka_clusters = environment_client.get_kafka_cluster_list(environment_id=environment["id"])
        
                try:
                    assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

                    logger.info("Kafka Clusters: %d", len(kafka_clusters))

                    environments_with_kafka_clusters[environment["id"]] = kafka_clusters
                except AssertionError as e:
                    logger.error(e)
                    logger.error("HTTP Status Code: %d, Error Message: %s, Kafka Clusters: %s", http_status_code, error_message, kafka_clusters)
                    return
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Environments: %s", http_status_code, error_message, environments)
            return

        for _, kafka_clusters in environments_with_kafka_clusters.items():
            kafka_cluster_count = len(kafka_clusters)
            for index, kafka_cluster in enumerate(kafka_clusters):
                http_status_code, error_message, api_key_pair = environment_client.create_kafka_api_key(kafka_cluster_id=kafka_cluster["id"], principal_id=principal_id)

                try:
                    assert http_status_code == HttpStatus.ACCEPTED, f"HTTP Status Code: {http_status_code}"

                    beautified = json.dumps(api_key_pair, indent=4, sort_keys=True)
                    logger.info("%d of %d Kafka API Key Pair: %s", index + 1, kafka_cluster_count, beautified)
                except AssertionError as e:
                    logger.error(e)
                    logger.error("HTTP Status Code: %d, Error Message: %s, Kafka API Key Pair: %s", http_status_code, error_message, api_key_pair)
                    return

                time.sleep(2)  # Wait for 2 seconds before deleting the API key.

                http_status_code, error_message = environment_client.delete_kafka_api_key(api_key=api_key_pair["key"])

                try:
                    assert http_status_code == HttpStatus.NO_CONTENT, f"HTTP Status Code: {http_status_code}"

                    logger.info("Successfully deleted Kafka API Key: %s", api_key_pair['key'])
                except AssertionError as e:
                    logger.error(e)
                    logger.error("HTTP Status Code: %d, Error Message: %s", http_status_code, error_message)
                    return

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