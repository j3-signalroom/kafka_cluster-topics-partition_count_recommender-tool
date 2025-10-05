import json
import logging
import os
from typing import Dict

from utilities import setup_logging
from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from aws_clients_python_lib.secrets_manager import get_secrets


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


def fetch_confluent_cloud_credential(use_aws_secrets_manager: bool) -> Dict:
    """Fetch Confluent Cloud configuration from environment variables or AWS Secrets Manager.

    Return(s):
        Dict: Confluent Cloud configuration dictionary.
    """
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            metrics_config, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if metrics_config == {}:
                logging.error("FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
                return {}

            logging.info("Retrieving the Confluent Cloud credentials from the AWS Secrets Manager.")
        else:
            metrics_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
            logging.info("Retrieving the Confluent Cloud credentials from the .env file.")

    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFLUENT CLOUD CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return {}
    

def fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id: str, 
                                                        environment_config: Dict, 
                                                        environment_filter: str | None = None, 
                                                        kafka_cluster_filter: str | None = None) -> list[Dict]:
    """Fetch Kafka credentials using Confluent Cloud API key.
    
    Args:
        principal_id (str): The Principal ID of the Confluent Cloud account running the tool
        environment_config (Dict): Confluent Cloud API credentials
        environment_filter (str | None): Optional filter for specific environment IDs
        kafka_cluster_filter (str | None): Optional filter for specific Kafka cluster IDs

    Return(s):
        list[Dict]: List of Kafka credentials dictionaries.
    """
    kafka_credentials = []

    # Instantiate the EnvironmentClient and IamClient classes.
    environment_client = EnvironmentClient(environment_config=environment_config)
    iam_client = IamClient(iam_config=environment_config)

    http_status_code, error_message, environments = environment_client.get_environment_list()
 
    if http_status_code != HttpStatus.OK:
        logger.error("FAILED TO RETRIEVE KAFKA CREDENTIALS FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
        return []
    else:
        # Filter environments if an environment filter is provided
        if environment_filter:
            environment_ids = [environment_id.strip() for environment_id in environment_filter.split(',')]
            environments = [environment for environment in environments if environment.get("id") in environment_ids]

        # Retrieve Kafka cluster credentials for each environment
        for environment in environments:
            http_status_code, error_message, kafka_clusters = environment_client.get_kafka_cluster_list(environment_id=environment.get("id"))

            if http_status_code != HttpStatus.OK:
                logger.error("FAILED TO RETRIEVE KAFKA CLUSTER LIST FOR ENVIRONMENT %s FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", environment.get('id'), error_message)
                return []
            else:
                # Filter Kafka clusters if a Kafka cluster filter is provided
                if kafka_cluster_filter:
                    # Filter Kafka clusters based on provided IDs
                    kafka_cluster_ids = [kafka_cluster_id.strip() for kafka_cluster_id in kafka_cluster_filter.split(',')]
                    kafka_clusters = [kafka_cluster for kafka_cluster in kafka_clusters if kafka_cluster.get("id") in kafka_cluster_ids]

                # Retrieve API key pair for each Kafka cluster
                for kafka_cluster in kafka_clusters:
                    http_status_code, error_message, api_key_pair = iam_client.create_api_key(resource_id=kafka_cluster.get("id"), 
                                                                                              principal_id=principal_id,
                                                                                              display_name=f"Temporary API Key for Kafka Cluster {kafka_cluster.get('display_name')} ({kafka_cluster.get('id')}) in Environment {environment.get('display_name')} for Principal {principal_id}",
                                                                                              description="This API key was created temporarily by the Kafka Cluster Topics Partition Count Recommender Tool to retrieve Kafka Cluster credentials.  It will be deleted automatically after use.")
                    
                    # If unable to retrieve the API key pair, log the error and attempt to clean up any previously created API keys
                    if http_status_code == HttpStatus.FORBIDDEN:
                        logger.warning("ACCESS FORBIDDEN: Unable to use Principal ID %s to retrieve Kafka Cluster credentials for Kafka Cluster %s in environment %s in cloud provider %s in region %s.", principal_id, kafka_cluster.get('id'), environment.get('id'), kafka_cluster.get('cloud_provider'), kafka_cluster.get('region_name'))
                        continue
                    elif http_status_code != HttpStatus.ACCEPTED:
                        logger.error("FAILED TO RETRIEVE KAFKA CLUSTER CREDENTIALS FOR KAFKA CLUSTER %s IN ENVIRONMENT %s FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_cluster.get('id'), environment.get('id'), error_message)

                        for kafka_credential in kafka_credentials:
                            if kafka_credential.get("kafka_cluster_id") == kafka_cluster.get("id"):
                                http_status_code, error_message = iam_client.delete_api_key(api_key=kafka_credential["sasl.username"])
                                if http_status_code != HttpStatus.ACCEPTED:
                                    logger.warning("FAILED TO DELETE KAFKA API KEY %s FOR KAFKA CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_credential['sasl.username'], kafka_credential['kafka_cluster_id'], error_message)
                                else:
                                    logger.info("KAFKA API KEY %s FOR KAFKA CLUSTER %s DELETED SUCCESSFULLY.", kafka_credential['sasl.username'], kafka_credential['kafka_cluster_id'])

                                kafka_credentials.remove(kafka_credential)
                        return []
                    else:
                        kafka_credentials.append({
                            "bootstrap.servers": kafka_cluster.get("kafka_bootstrap_endpoint"),
                            "sasl.username": api_key_pair.get("key"),
                            "sasl.password": api_key_pair.get("secret"),
                            "kafka_cluster_id": kafka_cluster.get("id")
                        })

        if not kafka_credentials:
            logging.error("NO KAFKA CREDENTIALS FOUND. PLEASE CHECK YOUR CONFIGURATION.")

        return kafka_credentials


def fetch_kafka_credentials_via_environment_variables(use_aws_secrets_manager: bool, kafka_cluster_filter: str | None = None) -> list[Dict]:
    """Fetch Kafka credentials from environment variable or AWS Secrets Manager.

    Args:
        use_aws_secrets_manager (bool): Whether to use AWS Secrets Manager for credentials retrieval
        kafka_cluster_filter (str | None): Optional filter for specific Kafka cluster IDs

    Return(s):
        list[Dict]: List of Kafka credentials dictionaries.
    """
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            # Retrieve Kafka API Key/Secret from AWS Secrets Manager
            kafka_api_secrets_paths = json.loads(os.getenv("KAFKA_API_SECRET_PATHS", "[]"))
            kafka_credentials = []
            for kafka_api_secrets_path in kafka_api_secrets_paths:
                settings, error_message = get_secrets(kafka_api_secrets_path["region_name"], kafka_api_secrets_path["secret_name"])
                if settings == {}:
                    logging.error("FAILED TO RETRIEVE KAFKA API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
                    return []
                else:
                    kafka_credentials.append({
                        "bootstrap.servers": settings.get("bootstrap.servers"),
                        "sasl.username": settings.get("sasl.username"),
                        "sasl.password": settings.get("sasl.password"),
                        "kafka_cluster_id": settings.get("kafka_cluster_id")
                    })
            logging.info("Retrieving the Kafka Cluster credentials from the AWS Secrets Manager.")
        else:
            kafka_credentials = json.loads(os.getenv("KAFKA_CREDENTIALS", "[]"))
            logging.info("Retrieving the Kafka Cluster credentials from the .env file.")
            
        if not kafka_credentials:
            logging.error("NO KAFKA CREDENTIALS FOUND. PLEASE CHECK YOUR CONFIGURATION.")
        else:
            if kafka_cluster_filter:
                kafka_cluster_ids = [kafka_cluster_id.strip() for kafka_cluster_id in kafka_cluster_filter.split(',')]
                kafka_credentials = [kafka_credential for kafka_credential in kafka_credentials if kafka_credential.get("kafka_cluster_id") in kafka_cluster_ids]

        return kafka_credentials
            
    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ KAFKA CREDENTIALS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return []
