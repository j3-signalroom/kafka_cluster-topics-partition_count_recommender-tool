import json
import logging
import os
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

from thread_safe_kafka_topics_analyzer import ThreadSafeKafkaTopicsAnalyzer
from utilities import setup_logging
from aws_clients_python_lib.secrets_manager import get_secrets
from constants import (DEFAULT_SAMPLING_DAYS, 
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
                       DEFAULT_CHARACTER_REPEAT)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup logging
logger = setup_logging()


def analyze_kafka_cluster(kafka_credential: Dict, config: Dict) -> bool:
    """Analyze a single Kafka cluster.
    
    Args:
        kafka_credential (Dict): Kafka cluster credentials
        config (Dict): Configuration parameters
        
    Returns:
        bool: True if analysis succeeded, False otherwise
    """
    try:
        # Instantiate the Kafka Topics Analyzer
        analyzer = ThreadSafeKafkaTopicsAnalyzer(
            kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
            bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
            kafka_api_key=kafka_credential.get("sasl.username"),
            kafka_api_secret=kafka_credential.get("sasl.password"),
            metrics_config=config['metrics_config']
        )

        # Analyze all topics in the Kafka cluster with multithreading
        success = analyzer.analyze_all_topics(
            include_internal=config['include_internal'],
            required_consumption_throughput_factor=config['required_consumption_throughput_factor'],
            use_sample_records=config['use_sample_records'],
            sampling_days=config['sampling_days'],
            sampling_batch_size=config['sampling_batch_size'],
            sampling_max_consecutive_nulls=config['sampling_max_consecutive_nulls'],
            sampling_timeout_seconds=config['sampling_timeout_seconds'],
            sampling_max_continuous_failed_batches=config['sampling_max_continuous_failed_batches'],
            topic_filter=config['topic_filter'],
            max_workers=config.get('max_workers_per_cluster', DEFAULT_MAX_WORKERS_PER_CLUSTER)
        )
        
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
    
    # Read the Kafka Cluster credentials from the environment variable or AWS Secrets Manager
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
                    return
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
            
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO RUN BECAUSE OF THE FOLLOWING ERROR: {e}")
        return

    if not kafka_credentials:
        logging.error("NO KAFKA CREDENTIALS FOUND. PLEASE CHECK YOUR CONFIGURATION.")
        return

    # Prepare configuration object
    config = {
        'metrics_config': metrics_config,
        'include_internal': include_internal,
        'required_consumption_throughput_factor': required_consumption_throughput_factor,
        'use_sample_records': use_sample_records,
        'sampling_days': sampling_days,
        'sampling_batch_size': sampling_batch_size,
        'sampling_max_consecutive_nulls': sampling_max_consecutive_nulls,
        'sampling_timeout_seconds': sampling_timeout_seconds,
        'sampling_max_continuous_failed_batches': sampling_max_continuous_failed_batches,
        'topic_filter': topic_filter,
        'max_workers_per_cluster': max_workers_per_cluster
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
        success = analyze_kafka_cluster(kafka_credentials[0], config)
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
                executor.submit(analyze_kafka_cluster, credential, config): credential.get("kafka_cluster_id", "unknown")
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
