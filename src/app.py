import json
import logging
from dotenv import load_dotenv
import os

from kafka_topics_analyzer import KafkaTopicsAnalyzer
from utilities import setup_logging
from aws_clients_python_lib.secrets_manager import get_secrets
from constants import (DEFAULT_SAMPLING_DAYS, 
                       DEFAULT_SAMPLING_BATCH_SIZE, 
                       DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR, 
                       DEFAULT_USE_SAMPLE_RECORDS,
                       DEFAULT_USE_AWS_SECRETS_MANAGER,
                       DEFAULT_INCLUDE_INTERNAL_TOPICS)
\

__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup logging
logger = setup_logging()


def main():
    # Load environment variables from .env file
    load_dotenv()
 
    # Read core configuration settings from environment variables
    try:
        required_consumption_throughput_factor = int(os.getenv("REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR", DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR))
        use_sample_records=os.getenv("USE_SAMPLE_RECORDS", DEFAULT_USE_SAMPLE_RECORDS) == "True"
        use_aws_secrets_manager = os.getenv("USE_AWS_SECRETS_MANAGER", DEFAULT_USE_AWS_SECRETS_MANAGER) == "True"
        include_internal=os.getenv("INCLUDE_INTERNAL_TOPICS", DEFAULT_INCLUDE_INTERNAL_TOPICS) == "True"
        sampling_days=int(os.getenv("SAMPLING_DAYS", DEFAULT_SAMPLING_DAYS))
        sampling_batch_size=int(os.getenv("SAMPLING_BATCH_SIZE", DEFAULT_SAMPLING_BATCH_SIZE))
        topic_filter=os.getenv("TOPIC_FILTER")
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: {e}") 
        return
    
    # Read Confluent Cloud credentials from environment variables or AWS Secrets Manager
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            metrics_config, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if metrics_config == {}:
                logging.error(f"FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
                return
            
            logging.info("Using AWS Secrets Manager for retrieving the Confluent Cloud credentials.")
        else:
            metrics_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
            logging.info("Using environment variables for retrieving the Confluent Cloud credentials.")

    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ CONFLUENT CLOUD CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: {e}") 
        return
    
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

        else:
            logging.info("Using environment variables for retrieving the Kafka Cluster credentials.")
            
            # Use environment variables directly
            kafka_credentials = json.loads(os.getenv("KAFKA_CREDENTIALS", "[]"))
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO RUN BECAUSE OF THE FOLLOWING ERROR: {e}")
        return
        
    if use_sample_records:
        logging.info(f"Using sample records for analysis with sample size: {sampling_batch_size:,.0f}")
    else:
        logging.info("Using Metrics API for analysis.")

    for kafka_credential in kafka_credentials:
        # Initialize recommender
        analyzer = KafkaTopicsAnalyzer(
            kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
            bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
            kafka_api_key=kafka_credential.get("sasl.username"),
            kafka_api_secret=kafka_credential.get("sasl.password"),
            metrics_config=metrics_config
        )

        # Analyze all topics        
        report_details = analyzer.analyze_all_topics(
            include_internal=include_internal,
            required_consumption_throughput_factor=required_consumption_throughput_factor,
            use_sample_records=use_sample_records,
            sampling_days=sampling_days,
            sampling_batch_size=sampling_batch_size,
            topic_filter=topic_filter
        )
        
        if not report_details:
            logging.error("NO TOPIC(S) FOUND OR ANALYSIS FAILED.")
    
    
if __name__ == "__main__":
    main()
