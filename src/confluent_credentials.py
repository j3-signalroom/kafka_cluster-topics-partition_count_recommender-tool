import json
import logging
import os
from typing import Dict

from utilities import setup_logging
from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.schema_registry_client import SchemaRegistryClient
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


def fetch_confluent_cloud_credential_via_env_file(use_aws_secrets_manager: bool) -> Dict:
    """Fetch Confluent Cloud credentials from .env file or AWS Secrets Manager.

    Return(s):
        Dict: Confluent Cloud credentials dictionary.
    """
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            cc_credentials, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if cc_credentials == {}:
                logging.error("FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
                return {}

            logging.info("Retrieving the Confluent Cloud credentials from the AWS Secrets Manager.")

            return cc_credentials
        else:
            cc_credentials = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
            logging.info("Retrieving the Confluent Cloud credentials from the .env file.")
            return cc_credentials

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

    http_status_code, error_message, environments = environment_client.get_environments()
 
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
            http_status_code, error_message, kafka_clusters = environment_client.get_kafka_clusters(environment_id=environment.get("id"))

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
                            "environment_id": environment.get("id"),
                            "bootstrap.servers": kafka_cluster.get("kafka_bootstrap_endpoint"),
                            "sasl.username": api_key_pair.get("key"),
                            "sasl.password": api_key_pair.get("secret"),
                            "kafka_cluster_id": kafka_cluster.get("id")
                        })

        if not kafka_credentials:
            logging.error("NO KAFKA CREDENTIALS FOUND. PLEASE CHECK YOUR CONFIGURATION.")

        return kafka_credentials


def fetch_schema_registry_via_confluent_cloud_api_key(principal_id: str,
                                                      resource_config: Dict, 
                                                      use_private_schema_registry: bool, 
                                                      environment_filter: str | None = None) -> Dict:
    """Fetch Schema Registry credentials using Confluent Cloud API key.
    
    Args:
        principal_id (str): The Principal ID of the Confluent Cloud account running the tool
        resource_config (Dict): Confluent Cloud API credentials
        use_private_schema_registry (bool): Whether to use private Schema Registry
        environment_filter (str | None): Optional filter for specific environment IDs

    Return(s):
        Dict: Schema Registry credentials dictionaries.
    """
    sr_credentials = {}

    # Instantiate the EnvironmentClient and IamClient classes.
    environment_client = EnvironmentClient(environment_config=resource_config)
    iam_client = IamClient(iam_config=resource_config)

    # Retrieve the list of environments
    http_status_code, error_message, environments = environment_client.get_environments()
    if http_status_code != HttpStatus.OK:
        logger.error("FAILED TO RETRIEVE SCHEMA REGISTRY CREDENTIALS FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
        return {}
    else:
        # Filter environments if an environment filter is provided
        if environment_filter:
            environment_ids = [environment_id.strip() for environment_id in environment_filter.split(',')]
            environments = [environment for environment in environments if environment.get("id") in environment_ids]

        # Initialize Schema Registry client
        resource_config["url"] = ""
        resource_config["api_key"] = ""
        resource_config["api_secret"] = ""
        sr_client = SchemaRegistryClient(resource_config)

        # Retrieve Kafka cluster credentials for each environment
        for environment in environments:
            http_status_code, error_message, sr_clusters = sr_client.get_schema_registry_cluster_list(environment_id=environment["id"])

            if http_status_code != HttpStatus.OK:
                logger.error("FAILED TO RETRIEVE SCHEMA REGISTRY CLUSTER LIST FOR ENVIRONMENT %s FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", environment.get('id'), error_message)
                return {}
            else:
                schema_registry_cluster_id = sr_clusters[0].get("id")

                http_status_code, error_message, api_key_pair = iam_client.create_api_key(resource_id=schema_registry_cluster_id, 
                                                                                          principal_id=principal_id,
                                                                                          display_name=f"Temporary API Key for Schema Registry Cluster {sr_clusters[0].get('display_name')} ({schema_registry_cluster_id}) in Environment {environment.get('display_name')} for Principal {principal_id}",
                                                                                          description="This API key was created temporarily by the Kafka Cluster Topics Partition Count Recommender Tool to retrieve Schema Registry credentials.  It will be deleted automatically after use.")
                
                # If unable to retrieve the API key pair, log the error and attempt to clean up any previously created API keys
                if http_status_code == HttpStatus.FORBIDDEN:
                    logger.warning("ACCESS FORBIDDEN: Unable to use Principal ID %s to retrieve Schema Registry credentials for Schema Registry %s in environment %s in cloud provider %s in region %s.", principal_id, schema_registry_cluster_id, environment.get('id'), sr_clusters[0].get('cloud_provider'), sr_clusters[0].get('region_name'))
                    continue
                elif http_status_code != HttpStatus.ACCEPTED:
                    logger.error("FAILED TO RETRIEVE SCHEMA REGISTRY CREDENTIALS FOR SCHEMA REGISTRY CLUSTER %s IN ENVIRONMENT %s FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", schema_registry_cluster_id, environment.get('id'), error_message)

                    for sr_credential_key, sr_credential_value in sr_credentials.items():
                        if sr_credential_value.get("schema_registry_cluster_id") == schema_registry_cluster_id:
                            http_status_code, error_message = iam_client.delete_api_key(api_key=sr_credential_value["basic.auth.user.info"].split(":")[0])
                            if http_status_code != HttpStatus.ACCEPTED:
                                logger.warning("FAILED TO DELETE SCHEMA REGISTRY API KEY %s FOR SCHEMA REGISTRY CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", sr_credential_value['basic.auth.user.info'].split(":")[0], schema_registry_cluster_id, error_message)
                            else:
                                logger.info("SCHEMA REGISTRY API KEY %s FOR SCHEMA REGISTRY CLUSTER %s DELETED SUCCESSFULLY.", sr_credential_value['basic.auth.user.info'].split(":")[0], schema_registry_cluster_id)

                            del sr_credentials[sr_credential_key]
                    return {}
                else:
                    if not use_private_schema_registry:
                        sr_credentials[environment.get("id")] = {"schema_registry_cluster_id": schema_registry_cluster_id,
                                                                 "url": sr_clusters[0].get("public_http_endpoint"),
                                                                 "basic.auth.user.info": f"{api_key_pair.get('key')}:{api_key_pair.get('secret')}"}
                    else:
                        sr_credentials[environment.get("id")] = {"schema_registry_cluster_id": schema_registry_cluster_id,
                                                                 "url": sr_clusters[0].get("private_regional_http_endpoints")[sr_clusters[0].get("region_name")],
                                                                 "basic.auth.user.info": f"{api_key_pair.get('key')}:{api_key_pair.get('secret')}"}

        if not sr_credentials:
            logging.error("NO SCHEMA REGISTRY CREDENTIALS FOUND. PLEASE CHECK YOUR CONFIGURATION.")

        return sr_credentials


def fetch_kafka_credentials_via_env_file(use_aws_secrets_manager: bool, kafka_cluster_filter: str | None = None) -> list[Dict]:
    """Fetch Kafka credentials from .env file or AWS Secrets Manager.

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
                        "kafka_cluster_id": settings.get("kafka_cluster_id"),
                        "environment_id": settings.get("environment_id")
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


def fetch_schema_registry_credentials_via_env_file(use_aws_secrets_manager: bool) -> Dict:
    """Fetch Schema Registry credentials from .env file or AWS Secrets Manager.

    Return(s):
        Dict: Schema Registry credentials dictionary.
    """
    try:
        sr_credentials = {}

        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            sr_api_secrets_path = json.loads(os.getenv("SCHEMA_REGISTRY_API_SECRET_PATH", "{}"))
            sr_credentials, error_message = get_secrets(sr_api_secrets_path["region_name"], sr_api_secrets_path["secret_name"])
            if sr_credentials == {}:
                logging.error("FAILED TO RETRIEVE SCHEMA REGISTRY API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
                return {}

            logging.info("Retrieving the Schema Registry credentials from the AWS Secrets Manager.")
        else:
            sr_credentials = json.loads(os.getenv("SCHEMA_REGISTRY_CREDENTIAL", "{}"))
            logging.info("Retrieving the Schema Registry credentials from the .env file.")
            return sr_credentials

    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ SCHEMA REGISTRY CREDENTIALS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return {}
