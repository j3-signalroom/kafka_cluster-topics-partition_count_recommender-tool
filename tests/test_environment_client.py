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
def environment_client():
    """Load the Confluent Cloud credentials from the environment variables."""
    load_dotenv()
    environment_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    yield EnvironmentClient(environment_config)


class TestEnvironmentClient:
    """Test Suite for the EnvironmentClient class."""

    def test_get_all_environments_with_kafka_clusters(self, environment_client):
        """Test the get_environments() and get_kafka_clusters() functions."""

        http_status_code, error_message, environments = environment_client.get_environments()
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            for environment_index, environment in enumerate(environments.values()):
                beautified = json.dumps(environment, indent=4, sort_keys=True)
                logger.info("%d of %d Environment: %s", environment_index + 1, len(environments), beautified)

                http_status_code, error_message, kafka_clusters = environment_client.get_kafka_clusters(environment_id=environment["id"])
        
                try:
                    assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

                    for kafka_cluster_index, kafka_cluster in enumerate(kafka_clusters.values()):
                        beautified = json.dumps(kafka_cluster, indent=4, sort_keys=True)
                        logger.info("Environment '%s' %d of %d Kafka Cluster: %s", environment["display_name"], kafka_cluster_index + 1, len(kafka_clusters), beautified)
                except AssertionError as e:
                    logger.error(e)
                    logger.error("HTTP Status Code: %d, Error Message: %s, Kafka Clusters: %s", http_status_code, error_message, kafka_clusters)
                    return
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Environments: %s", http_status_code, error_message, environments)
            return