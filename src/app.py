from datetime import datetime
import json
import logging
from dotenv import load_dotenv
import os
from typing import Dict, List

from kafka_topics_analyzer import KafkaTopicsAnalyzer
from utilities import setup_logging
from cc_clients_python_lib.http_status import HTTPStatus
from cc_clients_python_lib.metrics_client import MetricsClient, METRICS_CONFIG, KafkaMetric
from aws_clients_python_lib.secrets_manager import get_secrets
from constants import (DEFAULT_SAMPLING_DAYS, 
                       DEFAULT_SAMPLING_BATCH_SIZE, 
                       DEFAULT_CHARACTER_REPEAT, 
                       DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR, 
                       DEFAULT_USE_SAMPLE_RECORDS,
                       DEFAULT_USE_AWS_SECRETS_MANAGER,
                       DEFAULT_INCLUDE_INTERNAL_TOPICS)


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
        metrics_config = {}
        if use_aws_secrets_manager:
            logging.info("Using AWS Secrets Manager for retrieving the Confluent Cloud credentials.")

            # Retrieve Confluent Cloud API Key/Secret from AWS Secrets Manager
            cc_api_secrets_path = json.loads(os.getenv("CONFLUENT_CLOUD_API_SECRET_PATH", "{}"))
            settings, error_message = get_secrets(cc_api_secrets_path["region_name"], cc_api_secrets_path["secret_name"])
            if settings == {}:
                logging.error(f"FAILED TO RETRIEVE CONFLUENT CLOUD API KEY/SECRET FROM AWS SECRETS MANAGER BECAUSE THE FOLLOWING ERROR OCCURRED: {error_message}.")
                return
            else:
                metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = settings.get("confluent_cloud_api_key")
                metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = settings.get("confluent_cloud_api_secret")
        else:
            logging.info("Using environment variables for retrieving the Confluent Cloud credentials.")
            
            # Use environment variables directly
            metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = os.getenv("CONFLUENT_CLOUD_API_KEY")
            metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = os.getenv("CONFLUENT_CLOUD_API_SECRET")
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO READ CONFLUENT CLOUD CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: {e}") 
        return
    
    try:
        # Check if using AWS Secrets Manager for credentials retrieval
        if use_aws_secrets_manager:
            # Retrieve Kafka API Key/Secret from AWS Secrets Manager
            kafka_api_secrets_paths = json.loads(os.getenv("KAFKA_API_SECRET_PATH", "[]"))
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
            logging.info("Using environment variables for credentials retrieval.")
            
            # Use environment variables directly
            kafka_credentials = json.loads(os.getenv("KAFKA_CREDENTIALS", "[]"))
    except Exception as e:
        logging.error(f"THE APPLICATION FAILED TO RUN BECAUSE OF THE FOLLOWING ERROR: {e}")
        
    if use_sample_records:
        logging.info(f"Using sample records for analysis with sample size: {sampling_batch_size:,.0f}")
    else:
        logging.info("Using Metrics API for analysis.")

        # Instantiate the MetricsClient class.
        metrics_client = MetricsClient(metrics_config)

    for kafka_credential in kafka_credentials:
        # Initialize recommender
        analyzer = KafkaTopicsAnalyzer(
            bootstrap_server_uri=kafka_credential.get("bootstrap.servers"),
            kafka_api_key=kafka_credential.get("sasl.username"),
            kafka_api_secret=kafka_credential.get("sasl.password")
        )

        # Analyze all topics        
        results = analyzer.analyze_all_topics(
            include_internal=include_internal,
            use_sample_records=use_sample_records,
            sampling_days=sampling_days,
            sampling_batch_size=sampling_batch_size,
            topic_filter=topic_filter
        )
        
        if not results:
            logging.error("NO TOPIC(S) FOUND OR ANALYSIS FAILED.")
        else:
            # Generate report
            _generate_report(
                metrics_client=metrics_client,
                kafka_cluster_id=kafka_credential.get("kafka_cluster_id"),
                results=results,
                required_consumption_throughput_factor=required_consumption_throughput_factor,
                use_sample_records=use_sample_records
            )

            # Export detailed results to a JSON file
            json_file = f"{kafka_credential.get('kafka_cluster_id')}-topics-partition-count-recommender-app.json"
            with open(json_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logging.info(f"Exported detailed results to: {json_file}")
    

def _generate_report(metrics_client: MetricsClient, kafka_cluster_id: str, results: List[Dict], required_consumption_throughput_factor: float, use_sample_records: bool) -> None:
    """Generates and logs a report based on the analysis results.

    Args:
        metrics_client (MetricsClient): An instance of the MetricsClient class.
        kafka_cluster_id (str): The Kafka cluster ID.
        results (List[Dict]): A list of dictionaries containing analysis results for each topic.
        required_consumption_throughput_factor (float): The factor to multiply the consumer throughput by to determine required throughput.
        use_sample_records (bool): Whether sample records were used for analysis.

    Returns:
        None
    """
    # Generate report header
    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info("KAFKA TOPICS ANALYSIS RESULTS")
    logging.info(f"Analysis Timestamp: {datetime.now().isoformat()}")
    logging.info(f"Kafka Cluster ID: {kafka_cluster_id}")
    logging.info(f"Required Consumption Throughput Factor: {required_consumption_throughput_factor}")

    # Calculate details for each topic
    total_recommended_partitions = 0
    total_record_count = 0
    topic_details = []
    for result in results:
        # Extract necessary details
        kafka_topic_name = result['topic_name']
        partition_count = result['partition_count']
        is_compacted_str = result.get('is_compacted', False)
        
        if use_sample_records:
            # Use sample records to determine throughput
            record_count = result.get('total_record_count', 0)
            consumer_throughput = result.get('avg_bytes_per_record', 0) * record_count
            required_throughput = consumer_throughput * required_consumption_throughput_factor
        else:
            # Use Metrics API to determine throughput
            http_status_code, error_message, bytes_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_BYTES, kafka_cluster_id, kafka_topic_name)

            if http_status_code != HTTPStatus.OK:
                logging.error(f"Error retrieving 'RECEIVED BYTES' metric for topic {kafka_topic_name} because the following error occurred: {error_message}")
                result['error'] = error_message
                consumer_throughput = 0
                required_throughput = 0
                record_count = 0
            else:
                consumer_throughput = bytes_query_result.get('avg_total', 0)
                required_throughput = bytes_query_result.get('max_total', 0) * required_consumption_throughput_factor

                http_status_code, error_message, record_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_RECORDS, kafka_cluster_id, kafka_topic_name)

                if http_status_code != HTTPStatus.OK:
                    logging.error(f"Error retrieving 'RECEIVED RECORDS' metric for topic {kafka_topic_name} because the following error occurred: {error_message}")
                    result['error'] = error_message
                    record_count = 0
                else:
                    record_count = record_query_result.get('sum_total', 0)

        # Update total record count
        total_record_count += record_count

        # Calculate recommended partition count
        recommended_partition_count = round(required_throughput / consumer_throughput)
        total_recommended_partitions += recommended_partition_count if recommended_partition_count > 0 else 0

        # Format numbers with commas for thousands, and no decimal places
        consumer_throughput_str = f"{consumer_throughput:,.0f}"
        required_throughput_str = f"{required_throughput:,.0f}"
        recommended_partition_count_str = f"{recommended_partition_count:,.0f}" if recommended_partition_count > 0 else "N/A"
        record_count_str = f"{record_count:,.0f}"
        
        # Determine status
        if 'error' in result:
            status = "Error"
        elif record_count == 0:
            status = "Empty"
        else:
            status = "Active"
        
        # Append formatted details to the list
        topic_details.append(f"{kafka_topic_name:<40} {is_compacted_str:<15} {record_count_str:<12} {partition_count:<20} {required_throughput_str:<21} {consumer_throughput_str:<21} {recommended_partition_count_str:<25} {status:<10}")

    # Table header and details        
    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info(f"{'Topic Name':<40} {'Is Compacted?':<15} {'Records':<12} {'Current Partitions':<20} {'Required Throughput':<21} {'Consumer Throughput':<21} {'Recommended Partitions':<25} {'Status':<10}")
    logging.info("-" * DEFAULT_CHARACTER_REPEAT)
    for detail in topic_details:
        logging.info(detail)    

    # Summarize results
    total_topics = len(results)
    total_partitions = sum(result['partition_count'] for result in results)
    total_record_count = sum(result.get('total_record_count', 0) for result in results)

    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info("SUMMARY STATISTICS")
    logging.info("=" * DEFAULT_CHARACTER_REPEAT)
    logging.info(f"Total Topics: {total_topics}")

    if use_sample_records:
        active_topics = len([result for result in results if result.get('total_record_count', 0) > 0])
        logging.info(f"Active Topics: {active_topics} ({active_topics/total_topics*100:.1f}%)")

    logging.info(f"Total Partitions: {total_partitions}")
    logging.info(f"Total Recommended Partitions: {total_recommended_partitions}")
    logging.info(f"Total Records: {total_record_count:,}")
    logging.info(f"Average Partitions per Topic: {total_partitions/total_topics:.0f}")
    logging.info(f"Average Recommended Partitions per Topic: {total_recommended_partitions/total_topics:.0f}")

    
if __name__ == "__main__":
    main()
