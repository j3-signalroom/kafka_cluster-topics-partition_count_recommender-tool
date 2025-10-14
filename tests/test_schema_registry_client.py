import json
import logging
from dotenv import load_dotenv
import os
import pytest
from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.schema_registry_client import SchemaRegistryClient, SCHEMA_REGISTRY_CONFIG
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
def sr_client():
    """Load the Confluent Cloud credentials from the Schema Registry Cluster variables."""

    load_dotenv()
    schema_registry_cluster_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    schema_registry_cluster_config[SCHEMA_REGISTRY_CONFIG["url"]] = ""
    schema_registry_cluster_config[SCHEMA_REGISTRY_CONFIG["api_key"]] = ""
    schema_registry_cluster_config[SCHEMA_REGISTRY_CONFIG["api_secret"]] = ""
    yield SchemaRegistryClient(schema_registry_cluster_config)

@pytest.fixture
def environment_client():
    """Load the Confluent Cloud credentials from the IAM variables."""
    load_dotenv()
    environment_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    yield EnvironmentClient(environment_config)


class TestSchemaRegistryClient:
    """Test Suite for the SchemaRegistryClient class."""

    def test_getting_all_schema_registry_clusters(self, sr_client, environment_client):
        """Test the get_schema_registry_cluster_list() function."""

        http_status_code, error_message, environments = environment_client.get_environments()
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("Environments: %d", len(environments))

            for environment in environments:
                http_status_code, error_message, schema_registry_clusters = sr_client.get_schema_registry_cluster_list(environment_id=environment["id"])

                try:
                    assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

                    logger.info("Schema Registry Clusters: %d", len(schema_registry_clusters))

                    for schema_registry_cluster in schema_registry_clusters:
                        beautified = json.dumps(schema_registry_cluster, indent=4, sort_keys=True)
                        logger.info(beautified)
                except AssertionError as e:
                    logger.error(e)
                    logger.error("HTTP Status Code: %d, Error Message: %s, schema_registry_clusters: %s", http_status_code, error_message, schema_registry_clusters)
                    return
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, Environments: %s", http_status_code, error_message, environments)
            return