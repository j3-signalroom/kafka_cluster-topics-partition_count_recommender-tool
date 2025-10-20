from datetime import datetime, timezone
import logging
import os
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

from thread_safe_kafka_topics_analyzer import ThreadSafeKafkaTopicsAnalyzer
from utilities import setup_logging, get_app_version_number
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from confluent_credentials import (fetch_kafka_credentials_via_confluent_cloud_api_key,
                                   fetch_kafka_credentials_via_env_file,
                                   fetch_confluent_cloud_credential_via_env_file,
                                   fetch_schema_registry_via_confluent_cloud_api_key,
                                   fetch_schema_registry_credentials_via_env_file)
from constants import (DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS,
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


def _analyze_kafka_cluster(kafka_credential: Dict, config: Dict, sr_credential: Dict | None = None) -> bool:
    """Analyze a single Kafka cluster.
    
    Args:
        kafka_credential (Dict): Kafka cluster credentials
        sr_credential (Dict | None): Schema Registry credentials
        config (Dict): Configuration parameters
        
    Return(s):
        bool: True if analysis succeeded, False otherwise
    """
    try:
        # Instantiate the Kafka Topics Analyzer
        if sr_credential is not None:
            analyzer = ThreadSafeKafkaTopicsAnalyzer(kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
                                                     bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
                                                     kafka_api_key=kafka_credential.get("sasl.username"),
                                                     kafka_api_secret=kafka_credential.get("sasl.password"),
                                                     sr_url=sr_credential["url"],
                                                     sr_api_key_secret=sr_credential["basic.auth.user.info"])
        else:
            analyzer = ThreadSafeKafkaTopicsAnalyzer(kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
                                                     bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
                                                     kafka_api_key=kafka_credential.get("sasl.username"),
                                                     kafka_api_secret=kafka_credential.get("sasl.password"))    

        # Multithread the analysis of all topics in the Kafka cluster
        success = analyzer.analyze_all_topics(**config)

        # Log the result of the analysis
        if success:
            logging.info("KAFKA CLUSTER %s TOPIC ANALYSIS COMPLETED SUCCESSFULLY.", kafka_credential.get('kafka_cluster_id'))
        else:
            logging.error("KAFKA CLUSTER %s TOPIC ANALYSIS FAILED.", kafka_credential.get('kafka_cluster_id'))

        # Clean up the created Kafka API key(s) if it was created using Confluent Cloud API key
        if config["use_confluent_cloud_api_key_to_fetch_resource_credentials"]:
            # Instantiate the IamClient class.
            iam_client = IamClient(iam_config=config['metrics_config'])

            # Delete the Kafka API key created for this Kafka cluster
            http_status_code, error_message = iam_client.delete_api_key(api_key=kafka_credential["sasl.username"])
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
        use_confluent_cloud_api_key_to_fetch_resource_credentials = os.getenv("USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS", DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS) == "True"
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

        # Indicate whether to use Private Schema Registry
        use_private_schema_registry = os.getenv("USE_PRIVATE_SCHEMA_REGISTRY", "False") == "True"
    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return
    
    # Fetch Confluent Cloud credentials from environment variable or AWS Secrets Manager
    metrics_config = fetch_confluent_cloud_credential_via_env_file(use_aws_secrets_manager)
    if not metrics_config:
        return
    
    # Fetch Kafka credentials
    if use_confluent_cloud_api_key_to_fetch_resource_credentials:
        # Read the Kafka Cluster credentials using Confluent Cloud API key
        kafka_credentials = fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id, 
                                                                                metrics_config, 
                                                                                environment_filter, 
                                                                                kafka_cluster_filter)
    else:
        # Read the Kafka Cluster credentials from the environment variable or AWS Secrets Manager
        kafka_credentials = fetch_kafka_credentials_via_env_file(use_aws_secrets_manager)

    if not kafka_credentials:
        return
    
    # Fetch Schema Registry credentials
    if use_kafka_writer:
        if use_confluent_cloud_api_key_to_fetch_resource_credentials:
            # Read the Schema Registry credentials using Confluent Cloud API key
            sr_credentials = fetch_schema_registry_via_confluent_cloud_api_key(principal_id, 
                                                                               metrics_config, 
                                                                               use_private_schema_registry, 
                                                                               environment_filter)
        else:
            # Read the Schema Registry credentials from the environment variable or AWS Secrets Manager
            sr_credentials = fetch_schema_registry_credentials_via_env_file(use_aws_secrets_manager)
        if not sr_credentials:
            return

    # Prepare configuration object
    config = {
        'metrics_config': metrics_config,
        'use_confluent_cloud_api_key_to_fetch_resource_credentials': use_confluent_cloud_api_key_to_fetch_resource_credentials,
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
        'kafka_writer_topic_data_retention_in_days': kafka_writer_topic_data_retention_in_days,
        'utc_now': datetime.now(timezone.utc)
    }

    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info("MULTITHREADED KAFKA CLUSTER ANALYSIS STARTING")
    logging.info("-" * DEFAULT_CHARACTER_REPEAT)
    logging.info("Tool version number: %s", get_app_version_number())
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
        if use_kafka_writer:
            success = _analyze_kafka_cluster(kafka_credentials[0], config, sr_credential=sr_credentials[kafka_credentials[0]["environment_id"]])
        else:
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
            if use_kafka_writer:
                future_to_cluster = {
                    executor.submit(_analyze_kafka_cluster, kafka_credential, config, sr_credential=sr_credentials[kafka_credential["environment_id"]]): kafka_credential.get("kafka_cluster_id", "unknown")
                    for kafka_credential in kafka_credentials
                }
            else:
                future_to_cluster = {
                    executor.submit(_analyze_kafka_cluster, kafka_credential, config): kafka_credential.get("kafka_cluster_id", "unknown")
                    for kafka_credential in kafka_credentials
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

        # Clean up the created Schema Registry API key(s) if they were created using Confluent Cloud API key
        if use_kafka_writer:
            # Instantiate the IamClient class.
            iam_client = IamClient(iam_config=config['metrics_config'])

            # Delete all the Schema Registry API keys created for each Schema Registry instance
            for sr_credential in sr_credentials.values():
                http_status_code, error_message = iam_client.delete_api_key(api_key=sr_credential["basic.auth.user.info"].split(":")[0])
                if http_status_code != HttpStatus.NO_CONTENT:
                    logging.warning("FAILED TO DELETE SCHEMA REGISTRY API KEY %s FOR SCHEMA REGISTRY %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", sr_credential["basic.auth.user.info"].split(":")[0], sr_credential['schema_registry_cluster_id'], error_message)
                else:
                    logging.info("Schema Registry API key %s for Schema Registry %s deleted successfully.", sr_credential["basic.auth.user.info"].split(":")[0], sr_credential['schema_registry_cluster_id'])

        # Log final summary
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("MULTITHREADED ANALYSIS SUMMARY")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info("Tool version number: %s", get_app_version_number())
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
