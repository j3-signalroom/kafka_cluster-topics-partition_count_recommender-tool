import json
import logging
import os
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

from thread_safe_kafka_topics_analyzer import ThreadSafeKafkaTopicsAnalyzer
from utilities import setup_logging
from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.http_status import HttpStatus
from aws_clients_python_lib.secrets_manager import get_secrets
from constants import (DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_KAFKA_CREDENTIALS,
                       DEFAULT_SAMPLING_DAYS,
                       DEFAULT_SAMPLING_BATCH_SIZE,
                       DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES,
                       DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS,
                       DEFAULT_SAMPLING_TIMEOUT_SECONDS,
                       DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR,
                       DEFAULT_USE_SAMPLE_RECORDS,
                       DEFAULT_USE_AWS_SECRETS_MANAGER,
                       DEFAULT_INCLUDE_INTERNAL_TOPICS,
                       DEFAULT_MAX_CLUSTER_WORKERS,
                       DEFAULT_MAX_WORKERS_PER_CLUSTER,
                       DEFAULT_CHARACTER_REPEAT,
                       DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS,
                       DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD,
                       DEFAULT_USE_KAFKA_WRITER,
                       DEFAULT_KAFKA_WRITER_TOPIC_NAME,
                       DEFAULT_KAFKA_WRITER_TOPIC_PARTITION_COUNT,
                       DEFAULT_KAFKA_WRITER_TOPIC_REPLICATION_FACTOR,
                       DEFAULT_KAFKA_WRITER_TOPIC_DATA_RETENTION_IN_DAYS)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


def _fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id: str, 
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

    # Instantiate the EnvironmentClient class.
    environment_client = EnvironmentClient(environment_config=environment_config)

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
                    http_status_code, error_message, api_key_pair = environment_client.create_kafka_api_key(kafka_cluster_id=kafka_cluster.get("id"), principal_id=principal_id)
                    
                    # If unable to retrieve the API key pair, log the error and attempt to clean up any previously created API keys
                    if http_status_code == HttpStatus.FORBIDDEN:
                        logger.warning("ACCESS FORBIDDEN: Unable to use Principal ID %s to retrieve Kafka Cluster credentials for Kafka Cluster %s in environment %s in cloud provider %s in region %s.", principal_id, kafka_cluster.get('id'), environment.get('id'), kafka_cluster.get('cloud_provider'), kafka_cluster.get('region_name'))
                        continue
                    elif http_status_code != HttpStatus.ACCEPTED:
                        logger.error("FAILED TO RETRIEVE KAFKA CLUSTER CREDENTIALS FOR KAFKA CLUSTER %s IN ENVIRONMENT %s FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_cluster.get('id'), environment.get('id'), error_message)

                        for kafka_credential in kafka_credentials:
                            if kafka_credential.get("kafka_cluster_id") == kafka_cluster.get("id"):
                                http_status_code, error_message = environment_client.delete_kafka_api_key(api_key=kafka_credential["sasl.username"])
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


def _fetch_kafka_credentials_via_environment_variables(use_aws_secrets_manager: bool, kafka_cluster_filter: str | None = None) -> list[Dict]:
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
    

def _analyze_kafka_cluster(metrics_config: Dict, 
                           use_confluent_cloud_api_key_to_fetch_kafka_credentials: bool, 
                           kafka_credential: Dict, 
                           config: Dict) -> bool:
    """Analyze a single Kafka cluster.
    
    Args:
        metrics_config (Dict): Confluent Cloud API credentials
        use_confluent_cloud_api_key_to_fetch_kafka_credentials (bool): Whether the Kafka credentials were fetched using Confluent Cloud API key
        kafka_credential (Dict): Kafka cluster credentials
        config (Dict): Configuration parameters
        
    Return(s):
        bool: True if analysis succeeded, False otherwise
    """
    try:
        # Instantiate the Kafka Topics Analyzer
        analyzer = ThreadSafeKafkaTopicsAnalyzer(kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
                                                 bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
                                                 kafka_api_key=kafka_credential.get("sasl.username"),
                                                 kafka_api_secret=kafka_credential.get("sasl.password"),
                                                 metrics_config=config['metrics_config'])

        # Multithread the analysis of all topics in the Kafka cluster
        success = analyzer.analyze_all_topics(**config)

        # Log the result of the analysis
        if success:
            logging.info("KAFKA CLUSTER %s TOPIC ANALYSIS COMPLETED SUCCESSFULLY.", kafka_credential.get('kafka_cluster_id'))
        else:
            logging.error("KAFKA CLUSTER %s TOPIC ANALYSIS FAILED.", kafka_credential.get('kafka_cluster_id'))

        # Clean up the created Kafka API key(s) if it was created using Confluent Cloud API key
        if use_confluent_cloud_api_key_to_fetch_kafka_credentials:
            # Instantiate the EnvironmentClient class.
            environment_client = EnvironmentClient(environment_config=metrics_config)

            http_status_code, error_message = environment_client.delete_kafka_api_key(api_key=kafka_credential["sasl.username"])
            if http_status_code != HttpStatus.NO_CONTENT:
                logging.warning("FAILED TO DELETE KAFKA API KEY %s FOR KAFKA CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_credential['sasl.username'], kafka_credential['kafka_cluster_id'], error_message)
            else:
                logging.info("Kafka API key %s for Kafka Cluster %s deleted successfully.", kafka_credential['sasl.username'], kafka_credential['kafka_cluster_id'])

        return success
        
    except Exception as e:
        kafka_cluster_id = kafka_credential.get("kafka_cluster_id", "unknown")
        logging.error("CLUSTER %s: ANALYSIS FAILED WITH ERROR: %s", kafka_cluster_id, e)
        return False
    

def main():
    """Main application entry point with cluster-level multithreading support."""

    # Load environment variables from .env file
    load_dotenv()
 
    # Fetch environment variables non-credential configuration settings
    try:
        use_confluent_cloud_api_key_to_fetch_kafka_credentials = os.getenv("USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_KAFKA_CREDENTIALS", DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_KAFKA_CREDENTIALS) == "True"
        environment_filter = os.getenv("ENVIRONMENT_FILTER")
        kafka_cluster_filter = os.getenv("KAFKA_CLUSTER_FILTER")
        principal_id = os.getenv("PRINCIPAL_ID")
        required_consumption_throughput_factor = int(os.getenv("REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR", DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR))
        use_sample_records = os.getenv("USE_SAMPLE_RECORDS", DEFAULT_USE_SAMPLE_RECORDS) == "True"
        use_aws_secrets_manager = os.getenv("USE_AWS_SECRETS_MANAGER", DEFAULT_USE_AWS_SECRETS_MANAGER) == "True"
        include_internal = os.getenv("INCLUDE_INTERNAL_TOPICS", DEFAULT_INCLUDE_INTERNAL_TOPICS) == "True"
        sampling_days = int(os.getenv("SAMPLING_DAYS", DEFAULT_SAMPLING_DAYS))
        sampling_timeout_seconds = float(os.getenv("SAMPLING_TIMEOUT_SECONDS", DEFAULT_SAMPLING_TIMEOUT_SECONDS))
        sampling_max_consecutive_nulls = int(os.getenv("SAMPLING_MAX_CONSECUTIVE_NULLS", DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS))
        sampling_batch_size = int(os.getenv("SAMPLING_BATCH_SIZE", DEFAULT_SAMPLING_BATCH_SIZE))
        sampling_max_continuous_failed_batches = int(os.getenv("SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES", DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES))
        topic_filter = os.getenv("TOPIC_FILTER")
        min_recommended_partitions = int(os.getenv("MIN_RECOMMENDED_PARTITIONS", DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS))
        min_consumption_throughput = float(os.getenv("MIN_CONSUMPTION_THROUGHPUT", DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD))

        # Multithreading configuration
        max_cluster_workers = int(os.getenv("MAX_CLUSTER_WORKERS", DEFAULT_MAX_CLUSTER_WORKERS))  # Number of clusters to process concurrently
        max_workers_per_cluster = int(os.getenv("MAX_WORKERS_PER_CLUSTER", DEFAULT_MAX_WORKERS_PER_CLUSTER))  # Number of topics per cluster to process concurrently

        # Kafka writer configuration
        use_kafka_writer = os.getenv("USE_KAFKA_WRITER", DEFAULT_USE_KAFKA_WRITER) == "True"
        kafka_writer_topic_name = os.getenv("KAFKA_WRITER_TOPIC_NAME", DEFAULT_KAFKA_WRITER_TOPIC_NAME)
        kafka_writer_topic_partition_count = int(os.getenv("KAFKA_WRITER_TOPIC_PARTITION_COUNT", DEFAULT_KAFKA_WRITER_TOPIC_PARTITION_COUNT))
        kafka_writer_topic_replication_factor = int(os.getenv("KAFKA_WRITER_TOPIC_REPLICATION_FACTOR", DEFAULT_KAFKA_WRITER_TOPIC_REPLICATION_FACTOR))
        kafka_writer_topic_data_retention_in_days = int(os.getenv("KAFKA_WRITER_TOPIC_DATA_RETENTION_IN_DAYS", DEFAULT_KAFKA_WRITER_TOPIC_DATA_RETENTION_IN_DAYS))
    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return
    
    # Read Confluent Cloud credentials from environment variable or AWS Secrets Manager
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            metrics_config, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if metrics_config == {}:
                logging.error("FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", error_message)
                return

            logging.info("Retrieving the Confluent Cloud credentials from the AWS Secrets Manager.")
        else:
            metrics_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
            logging.info("Retrieving the Confluent Cloud credentials from the .env file.")

    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFLUENT CLOUD CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return
    
    # Fetch Kafka credentials
    if use_confluent_cloud_api_key_to_fetch_kafka_credentials:
        # Read the Kafka Cluster credentials using Confluent Cloud API key
        kafka_credentials = _fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id, 
                                                                                  metrics_config, 
                                                                                  environment_filter, 
                                                                                  kafka_cluster_filter)
    else:
        # Read the Kafka Cluster credentials from the environment variable or AWS Secrets Manager
        kafka_credentials = _fetch_kafka_credentials_via_environment_variables(use_aws_secrets_manager)

    if not kafka_credentials:
        return

    # Prepare configuration object
    config = {
        'metrics_config': metrics_config,
        'use_confluent_cloud_api_key_to_fetch_kafka_credentials': use_confluent_cloud_api_key_to_fetch_kafka_credentials,
        'environment_filter': environment_filter,
        'kafka_cluster_filter': kafka_cluster_filter,
        'principal_id': principal_id,
        'include_internal': include_internal,
        'required_consumption_throughput_factor': required_consumption_throughput_factor,
        'use_sample_records': use_sample_records,
        'sampling_days': sampling_days,
        'sampling_batch_size': sampling_batch_size,
        'sampling_max_consecutive_nulls': sampling_max_consecutive_nulls,
        'sampling_timeout_seconds': sampling_timeout_seconds,
        'sampling_max_continuous_failed_batches': sampling_max_continuous_failed_batches,
        'topic_filter': topic_filter,
        'max_workers_per_cluster': max_workers_per_cluster,
        'min_recommended_partitions': min_recommended_partitions,
        'min_consumption_throughput': min_consumption_throughput,
        'use_kafka_writer': use_kafka_writer,
        'kafka_writer_topic_name': kafka_writer_topic_name,
        'kafka_writer_topic_partition_count': kafka_writer_topic_partition_count,
        'kafka_writer_topic_replication_factor': kafka_writer_topic_replication_factor,
        'kafka_writer_topic_data_retention_in_days': kafka_writer_topic_data_retention_in_days
    }

    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info("MULTITHREADED KAFKA CLUSTER ANALYSIS STARTING")
    logging.info("-" * DEFAULT_CHARACTER_REPEAT)
    logging.info("Number of Kafka clusters to analyze: %d", len(kafka_credentials))
    logging.info("Max concurrent Kafka clusters: %d", max_cluster_workers)
    logging.info("Max concurrent topics per cluster: %d", max_workers_per_cluster)
    logging.info("Analysis method: %s", "Record sampling" if use_sample_records else "Metrics API")
    logging.info("Kafka writer enabled: %s", use_kafka_writer)
    logging.info("Kafka writer topic name: %s", kafka_writer_topic_name)
    logging.info("Kafka writer topic partition count: %d", kafka_writer_topic_partition_count)
    logging.info("Kafka writer topic replication factor: %d", kafka_writer_topic_replication_factor)
    logging.info("Kafka writer topic data retention (in days): %d", kafka_writer_topic_data_retention_in_days)
    logging.info("=" * DEFAULT_CHARACTER_REPEAT)

    # Analyze Kafka clusters concurrently if more than one cluster
    if len(kafka_credentials) == 1:
        # Single Kafka cluster.  No need for cluster-level threading
        success = _analyze_kafka_cluster(metrics_config, 
                                         use_confluent_cloud_api_key_to_fetch_kafka_credentials, 
                                         kafka_credentials[0], 
                                         config)
        if success:
            logging.info("SINGLE KAFKA CLUSTER ANALYSIS COMPLETED SUCCESSFULLY.")
        else:
            logging.error("SINGLE KAFKA CLUSTER ANALYSIS FAILED.")
    else:
        # Multiple Kafka clusters.  Use ThreadPoolExecutor for cluster-level concurrency
        successful_clusters = 0
        failed_clusters = 0
        
        with ThreadPoolExecutor(max_workers=max_cluster_workers) as executor:
            # Submit Kafka cluster analysis tasks
            future_to_cluster = {
                executor.submit(_analyze_kafka_cluster, metrics_config, use_confluent_cloud_api_key_to_fetch_kafka_credentials, credential, config): credential.get("kafka_cluster_id", "unknown")
                for credential in kafka_credentials
            }
            
            # Process completed Kafka cluster analyses
            for future in as_completed(future_to_cluster):
                kafka_cluster_id = future_to_cluster[future]
                try:
                    success = future.result()
                    if success:
                        successful_clusters += 1
                        logging.info("KAFKA CLUSTER %s: ANALYSIS COMPLETED", kafka_cluster_id)
                    else:
                        failed_clusters += 1
                        logging.error("KAFKA CLUSTER %s: ANALYSIS FAILED", kafka_cluster_id)
                except Exception as e:
                    failed_clusters += 1
                    logging.error("KAFKA CLUSTER %s: ANALYSIS FAILED WITH EXCEPTION: %s", kafka_cluster_id, e)

        # Log final summary
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("MULTITHREADED ANALYSIS SUMMARY")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info("Total Kafka clusters analyzed: %d", len(kafka_credentials))
        logging.info("Successful Kafka cluster analyses: %d", successful_clusters)
        logging.info("Failed Kafka cluster analyses: %d", failed_clusters)

        if successful_clusters == len(kafka_credentials):
            logging.info("ALL KAFKA CLUSTER ANALYSES COMPLETED SUCCESSFULLY.")
        elif successful_clusters > 0:
            logging.warning("PARTIAL SUCCESS: %d of %d KAFKA CLUSTERS ANALYZED SUCCESSFULLY.", successful_clusters, len(kafka_credentials))
        else:
            logging.error("ALL KAFKA CLUSTER ANALYSES FAILED.")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    

# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()
