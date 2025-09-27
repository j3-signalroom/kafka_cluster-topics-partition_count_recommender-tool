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
                       DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup logging
logger = setup_logging()


def _fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id: str, 
                                                         environment_config: Dict, 
                                                         environment_filter: str | None, 
                                                         kafka_cluster_filter: str | None) -> list[Dict]:
    """Fetch Kafka credentials using Confluent Cloud API key.
    
    Args:
        principal_id (str): The owner of the application's Confluent Cloud principal ID
        environment_config (Dict): Confluent Cloud API credentials
        environment_filter (str | None): Optional filter for specific environment IDs
        kafka_cluster_filter (str | None): Optional filter for specific Kafka cluster IDs

    Returns:
        list[Dict]: List of Kafka credentials dictionaries.
    """
    kafka_credentials = []
    # Instantiate the EnvironmentClient class.
    environment_client = EnvironmentClient(environment_config=environment_config)

    http_status_code, error_message, environments = environment_client.get_environment_list()
 
    if http_status_code != HttpStatus.OK:
        logger.error(f"FAILED TO RETRIEVE KAFKA CREDENTIALS FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
        return []
    else:
        # Filter environments if an environment filter is provided
        if environment_filter is None:
            filter_environments = environments
        else:
            environment_ids = [environment_id.strip() for environment_id in environment_filter.split(',')]
            filter_environments = [environment for environment in environments if environment.get("id") in environment_ids]
        
        # Retrieve Kafka cluster credentials for each environment
        for environment in filter_environments:
            http_status_code, error_message, kafka_clusters = environment_client.get_kafka_cluster_list(environment_id=environment.get("id"))

            if http_status_code != HttpStatus.OK:
                logger.error(f"FAILED TO RETRIEVE KAFKA CLUSTER LIST FOR ENVIRONMENT {environment.get('id')} FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
                return []
            else:
                # Filter Kafka clusters if a Kafka cluster filter is provided
                if kafka_cluster_filter is None:
                    filter_kafka_clusters = kafka_clusters
                else:
                    # Filter Kafka clusters based on provided IDs
                    kafka_cluster_ids = [kafka_cluster_id.strip() for kafka_cluster_id in kafka_cluster_filter.split(',')]
                    filter_kafka_clusters = [kafka_cluster for kafka_cluster in kafka_clusters if kafka_cluster.get("id") in kafka_cluster_ids]

                # Retrieve API key pair for each Kafka cluster
                for kafka_cluster in filter_kafka_clusters:
                    http_status_code, error_message, api_key_pair = environment_client.create_kafka_api_key(kafka_cluster_id=kafka_cluster.get("id"), 
                                                                                                            principal_id=principal_id)
                    
                    # If unable to retrieve the API key pair, log the error and attempt to clean up any previously created API keys
                    if http_status_code != HttpStatus.OK:
                        logger.error(f"FAILED TO RETRIEVE KAFKA CLUSTER CREDENTIALS FOR KAFKA CLUSTER {kafka_cluster.get('id')} IN ENVIRONMENT {environment.get('id')} FROM CONFLUENT CLOUD BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")

                        for kafka_credential in kafka_credentials:
                            if kafka_credential.get("kafka_cluster_id") == kafka_cluster.get("id"):
                                http_status_code, error_message = environment_client.delete_kafka_api_key(api_key=kafka_credential["sasl.username"])
                                if http_status_code != HttpStatus.NO_CONTENT:
                                    logger.warning(f"FAILED TO DELETE KAFKA API KEY {kafka_credential['sasl.username']} FOR KAFKA CLUSTER {kafka_credential['kafka_cluster_id']} BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
                                else:
                                    logger.info(f"KAFKA API KEY {kafka_credential['sasl.username']} FOR KAFKA CLUSTER {kafka_credential['kafka_cluster_id']} DELETED SUCCESSFULLY.")

                                kafka_credentials.remove(kafka_credential)
                        return []
                    else:
                        kafka_credentials.append({
                            "bootstrap.servers": kafka_cluster.get("kafka_bootstrap_endpoint"),
                            "sasl.username": api_key_pair.get("key"),
                            "sasl.password": api_key_pair.get("secret"),
                            "kafka_cluster_id": kafka_cluster.get("id")
                        })

        return kafka_credentials


def _fetch_kafka_credentials_via_environment_variables(use_aws_secrets_manager: bool, kafka_cluster_filter: str | None) -> list[Dict]:
    """Fetch Kafka credentials from environment variable or AWS Secrets Manager.

    Args:
        use_aws_secrets_manager (bool): Whether to use AWS Secrets Manager for credentials retrieval
        kafka_cluster_filter (str | None): Optional filter for specific Kafka cluster IDs

    Returns:
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
                    logging.error(f"FAILED TO RETRIEVE KAFKA API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
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
            if kafka_cluster_filter is not None:
                kafka_cluster_ids = [kafka_cluster_id.strip() for kafka_cluster_id in kafka_cluster_filter.split(',')]
                kafka_credentials = [kafka_credential for kafka_credential in kafka_credentials if kafka_credential.get("kafka_cluster_id") in kafka_cluster_ids]

        return kafka_credentials
            
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ KAFKA CREDENTIALS BECAUSE OF THE FOLLOWING ERROR: {e}")
        return []
    

def _analyze_kafka_cluster(kafka_credential: Dict, config: Dict) -> bool:
    """Analyze a single Kafka cluster.
    
    Args:
        kafka_credential (Dict): Kafka cluster credentials
        config (Dict): Configuration parameters
        
    Returns:
        bool: True if analysis succeeded, False otherwise
    """
    try:
        # Instantiate the Kafka Topics Analyzer
        analyzer = ThreadSafeKafkaTopicsAnalyzer(kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
                                                 bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
                                                 kafka_api_key=kafka_credential.get("sasl.username"),
                                                 kafka_api_secret=kafka_credential.get("sasl.password"),
                                                 metrics_config=config['metrics_config'])

        # Analyze all topics in the Kafka cluster with multithreading
        success = analyzer.analyze_all_topics(use_confluent_cloud_api_key_to_fetch_kafka_credentials=config['use_confluent_cloud_api_key_to_fetch_kafka_credentials'],
                                              environment_filter=config['environment_filter'],
                                              kafka_cluster_filter=config['kafka_cluster_filter'],
                                              principal_id=config['principal_id'],
                                              include_internal=config['include_internal'],
                                              required_consumption_throughput_factor=config['required_consumption_throughput_factor'],
                                              use_sample_records=config['use_sample_records'],
                                              sampling_days=config['sampling_days'],
                                              sampling_batch_size=config['sampling_batch_size'],
                                              sampling_max_consecutive_nulls=config['sampling_max_consecutive_nulls'],
                                              sampling_timeout_seconds=config['sampling_timeout_seconds'],
                                              sampling_max_continuous_failed_batches=config['sampling_max_continuous_failed_batches'],
                                              topic_filter=config['topic_filter'],
                                              max_workers=config.get('max_workers_per_cluster', DEFAULT_MAX_WORKERS_PER_CLUSTER),
                                              min_recommended_partitions=config.get('min_recommended_partitions', DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS),
                                              min_consumption_throughput=config.get('min_consumption_throughput', DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD))
        
        kafka_cluster_id = kafka_credential.get("kafka_cluster_id", "unknown")
        if success:
            logging.info(f"KAFKA CLUSTER {kafka_cluster_id}: TOPIC ANALYSIS COMPLETED SUCCESSFULLY.")
        else:
            logging.error(f"KAFKA CLUSTER {kafka_cluster_id}: TOPIC ANALYSIS FAILED.")
            
        return success
        
    except Exception as e:
        kafka_cluster_id = kafka_credential.get("kafka_cluster_id", "unknown")
        logging.error(f"CLUSTER {kafka_cluster_id}: ANALYSIS FAILED WITH ERROR: {e}")
        return False
    

def main():
    """Main application entry point with cluster-level multithreading support."""
    # Load environment variables from .env file
    load_dotenv()
 
    # Read core configuration settings from environment variables
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

    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: {e}") 
        return
    
    # Read Confluent Cloud credentials from environment variable or AWS Secrets Manager
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            metrics_config, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if metrics_config == {}:
                logging.error(f"FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
                return

            logging.info("Retrieving the Confluent Cloud credentials from the AWS Secrets Manager.")
        else:
            metrics_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
            logging.info("Retrieving the Confluent Cloud credentials from the .env file.")

    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ CONFLUENT CLOUD CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: {e}") 
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
        'kafka_credentials': kafka_credentials,
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
        'min_consumption_throughput': min_consumption_throughput
    }

    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info("MULTITHREADED KAFKA CLUSTER ANALYSIS STARTING")
    logging.info("-" * DEFAULT_CHARACTER_REPEAT)
    logging.info(f"Number of Kafka clusters to analyze: {len(kafka_credentials)}")
    logging.info(f"Max concurrent Kafka clusters: {max_cluster_workers}")
    logging.info(f"Max concurrent topics per cluster: {max_workers_per_cluster}")
    logging.info(f'Analysis method: {"Record sampling" if use_sample_records else "Metrics API"}')
    logging.info("=" * DEFAULT_CHARACTER_REPEAT)

    # Analyze Kafka clusters concurrently if more than one cluster
    if len(kafka_credentials) == 1:
        # Single Kafka cluster.  No need for cluster-level threading
        success = _analyze_kafka_cluster(kafka_credentials[0], config)
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
                executor.submit(_analyze_kafka_cluster, credential, config): credential.get("kafka_cluster_id", "unknown")
                for credential in kafka_credentials
            }
            
            # Process completed Kafka cluster analyses
            for future in as_completed(future_to_cluster):
                kafka_cluster_id = future_to_cluster[future]
                try:
                    success = future.result()
                    if success:
                        successful_clusters += 1
                        logging.info(f"KAFKA CLUSTER {kafka_cluster_id}: ANALYSIS COMPLETED")
                    else:
                        failed_clusters += 1
                        logging.error(f"KAFKA CLUSTER {kafka_cluster_id}: ANALYSIS FAILED")
                except Exception as e:
                    failed_clusters += 1
                    logging.error(f"KAFKA CLUSTER {kafka_cluster_id}: ANALYSIS FAILED WITH EXCEPTION: {e}")

        # Log final summary
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("MULTITHREADED ANALYSIS SUMMARY")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Total Kafka clusters analyzed: {len(kafka_credentials)}")
        logging.info(f"Successful Kafka cluster analyses: {successful_clusters}")
        logging.info(f"Failed Kafka cluster analyses: {failed_clusters}")

        if successful_clusters == len(kafka_credentials):
            logging.info("ALL KAFKA CLUSTER ANALYSES COMPLETED SUCCESSFULLY.")
        elif successful_clusters > 0:
            logging.warning(f"PARTIAL SUCCESS: {successful_clusters}/{len(kafka_credentials)} KAFKA CLUSTERS ANALYZED SUCCESSFULLY.")
        else:
            logging.error("ALL KAFKA CLUSTER ANALYSES FAILED.")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    
    
if __name__ == "__main__":
    main()
