from datetime import datetime
import json
import logging
from dotenv import load_dotenv
import os

from KafkaTopicsAnalyzer import KafkaTopicsAnalyzer
from utilities import setup_logging
from cc_clients_python_lib.metrics_client import MetricsClient, METRICS_CONFIG, KafkaMetric
from aws_clients_python_lib.secrets_manager import get_secrets
from constants import DEFAULT_SAMPLING_DAYS, DEFAULT_SAMPLING_BATCH_SIZE, DEFAULT_CHARACTER_REPEAT


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
 
    try:
        required_consumption_throughput_factor = int(os.getenv("REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR", "5"))
        use_sample_records=os.getenv("USE_SAMPLE_RECORDS", "True") == "True"
        
        # Check if using AWS Secrets Manager for credentials retrieval
        metrics_config = {}
        if os.getenv("USE_AWS_SECRETS_MANAGER", "False") == "True":
            # Retrieve Confluent Cloud API Key/Secret from AWS Secrets Manager
            cc_secrets_path = os.getenv("CONFLUENT_CLOUD_API_KEY_AWS_SECRETS")
            settings, error_message = get_secrets(os.environ['AWS_REGION_NAME'], cc_secrets_path)
            if settings == {}:
                metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = os.getenv("CONFLUENT_CLOUD_API_KEY")
                metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = os.getenv("CONFLUENT_CLOUD_API_SECRET")
            else:
                metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = settings.get("confluent_cloud_api_key")
                metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = settings.get("confluent_cloud_api_secret")

            # Retrieve Kafka API Key/Secret from AWS Secrets Manager
            kafka_secrets_path = os.getenv("KAFKA_API_KEY_AWS_SECRETS")
            settings, error_message = get_secrets(os.environ['AWS_REGION_NAME'], kafka_secrets_path)
            if settings == {}:
                kafka_cluster_id = os.getenv("KAFKA_CLUSTER_ID")
                bootstrap_server_uri=os.getenv("BOOTSTRAP_SERVER_URI")
                kafka_api_key=os.getenv("KAFKA_API_KEY")
                kafka_api_secret=os.getenv("KAFKA_API_SECRET")
            else:
                kafka_cluster_id = settings.get("kafka_cluster_id")
                bootstrap_server_uri=settings.get("bootstrap.servers")
                kafka_api_key=settings.get("sasl.username")
                kafka_api_secret=settings.get("sasl.password")
        else:
            # Use environment variables directly
            metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = os.getenv("CONFLUENT_CLOUD_API_KEY")
            metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = os.getenv("CONFLUENT_CLOUD_API_SECRET")
            kafka_cluster_id = os.getenv("KAFKA_CLUSTER_ID")
            bootstrap_server_uri=os.getenv("BOOTSTRAP_SERVER_URI")
            kafka_api_key=os.getenv("KAFKA_API_KEY")
            kafka_api_secret=os.getenv("KAFKA_API_SECRET")

        if use_sample_records:
            logging.info(f"Using sample records for analysis with sample size: {os.getenv('SAMPLING_BATCH_SIZE', DEFAULT_SAMPLING_BATCH_SIZE)}")
        else:
            logging.info("Using Metrics API for analysis.")

            # Instantiate the MetricsClient class.
            metrics_client = MetricsClient(metrics_config)

        # Initialize recommender
        analyzer = KafkaTopicsAnalyzer(
            bootstrap_server_uri=bootstrap_server_uri,
            kafka_api_key=kafka_api_key,
            kafka_api_secret=kafka_api_secret
        )

        # Analyze all topics        
        results = analyzer.analyze_all_topics(
            include_internal=os.getenv("INCLUDE_INTERNAL_TOPICS", "False") == "True",
            use_sample_records=use_sample_records,
            sampling_days=int(os.getenv("SAMPLING_DAYS", DEFAULT_SAMPLING_DAYS)),
            sampling_batch_size=int(os.getenv("SAMPLING_BATCH_SIZE", DEFAULT_SAMPLING_BATCH_SIZE)),            
            topic_filter=os.getenv("TOPIC_FILTER")
        )
        
        if not results:
            logging.error("No topics found or analysis failed.")
            return
        
        # Display results
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("KAFKA TOPICS ANALYSIS RESULTS")
        logging.info(f"Analysis Timestamp: {datetime.now().isoformat()}")
        logging.info(f"Kafka Cluster ID: {kafka_cluster_id}")
        logging.info(f"Required Consumption Throughput Factor: {required_consumption_throughput_factor}")
        
        total_recommended_partitions = 0
        total_record_count = 0

        # Sort results by topic name
        topic_details = []
        for result in sorted(results, key=lambda x: x['topic_name']):
            kafka_topic_name = result['topic_name']
            partition_count = result['partition_count']
            
            if use_sample_records:
                record_count = result.get('total_record_count', 0)
                consumer_throughput = result.get('avg_bytes_per_record', 0) * record_count
                required_throughput = consumer_throughput * required_consumption_throughput_factor
            else:
                http_status_code, error_message, bytes_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_BYTES, kafka_cluster_id, kafka_topic_name)
                consumer_throughput = bytes_query_result.get('avg_total', 0)
                required_throughput = bytes_query_result.get('max_total', 0) * required_consumption_throughput_factor

                http_status_code, error_message, record_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_RECORDS, kafka_cluster_id, kafka_topic_name)
                record_count = record_query_result.get('sum_total', 0)

            total_record_count += record_count

            # Calculate recommended partition count
            recommended_partition_count = round(required_throughput / consumer_throughput)
            total_recommended_partitions += recommended_partition_count if recommended_partition_count > 0 else 0

            consumer_throughput_str = f"{consumer_throughput:,.0f}"
            required_throughput_str = f"{required_throughput:,.0f}"
            recommended_partition_count_str = f"{recommended_partition_count:,.0f}" if recommended_partition_count > 0 else "N/A"
            
            # Status
            if 'error' in result:
                status = "Error"
            elif record_count == 0:
                status = "Empty"
            else:
                status = "Active"
            
            # Format numbers with commas
            messages_str = f"{record_count:,.0f}"
            topic_details.append(f"{kafka_topic_name:<40} {messages_str:<12} {partition_count:<12} {required_throughput_str:<21} {consumer_throughput_str:<21} {recommended_partition_count_str:<25} {status:<10}")

        # Table header and details        
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"{'Topic Name':<40} {'Messages':<12} {'Partitions':<12} {'Required Throughput':<21} {'Consumer Throughput':<21} {'Recommended Partitions':<25} {'Status':<10}")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        for detail in topic_details:
            logging.info(detail)    

        # Summary statistics
        total_topics = len(results)
        total_partitions = sum(r['partition_count'] for r in results)
        total_record_count = sum(r.get('total_record_count', 0) for r in results)

        if use_sample_records:
            active_topics = len([r for r in results if r.get('total_record_count', 0) > 0])

        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("SUMMARY STATISTICS")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Total Topics: {total_topics}")

        if use_sample_records:
            logging.info(f"Active Topics: {active_topics} ({active_topics/total_topics*100:.1f}%)")

        logging.info(f"Total Partitions: {total_partitions}")
        logging.info(f"Total Recommended Partitions: {total_recommended_partitions}")
        logging.info(f"Total Records: {total_record_count:,}")
        logging.info(f"Average Partitions per Topic: {total_partitions/total_topics:.0f}")
        logging.info(f"Average Recommended Partitions per Topic: {total_recommended_partitions/total_topics:.0f}")

        # Export detailed results to JSON
        output_file = "kafka-cluster-topics-partition-count-recommender-app.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logging.info(f"\nDetailed results exported to: {output_file}")
        
    except Exception as e:
        logging.error(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()
