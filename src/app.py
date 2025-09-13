from datetime import datetime
import json
import logging
from dotenv import load_dotenv
import os
from typing import Final

from KafkaTopicsAnalyzer import KafkaTopicsAnalyzer
from utilities import setup_logging
from cc_clients_python_lib.metrics_client import MetricsClient, METRICS_CONFIG, KafkaMetric


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Default configuration constants
DEFAULT_SAMPLE_SIZE: Final[int] = 1000
DEFAULT_CHARACTER_REPEAT: Final[int] = 140

# Setup logging
logger = setup_logging()


def main():
    # Load environment variables from .env file
    load_dotenv()
 
    try:
        metrics_config = {}
        metrics_config[METRICS_CONFIG["confluent_cloud_api_key"]] = os.getenv("CONFLUENT_CLOUD_API_KEY")
        metrics_config[METRICS_CONFIG["confluent_cloud_api_secret"]] = os.getenv("CONFLUENT_CLOUD_API_SECRET")

        kafka_cluster_id = os.getenv("KAFKA_CLUSTER_ID")

        # Instantiate the MetricsClient class.
        metrics_client = MetricsClient(metrics_config)

        # Initialize recommender
        analyzer = KafkaTopicsAnalyzer(
            bootstrap_server_uri=os.getenv("BOOTSTRAP_SERVER_URI"),
            kafka_api_key=os.getenv("KAFKA_API_KEY"),
            kafka_api_secret=os.getenv("KAFKA_API_SECRET")
        )

        # Analyze all topics
        results = analyzer.analyze_all_topics(
            include_internal=os.getenv("INCLUDE_INTERNAL_TOPICS", "False") == "True",
            sample_records=os.getenv("SAMPLE_RECORDS", "True") == "True",
            sample_size=int(os.getenv("SAMPLE_SIZE", DEFAULT_SAMPLE_SIZE)),
            topic_filter=os.getenv("TOPIC_FILTER")
        )
        
        if not results:
            logging.error("No topics found or analysis failed.")
            return
        
        # Display results
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("KAFKA TOPICS ANALYSIS RESULTS")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)

        # Table header
        header = f"{'Topic Name':<40} {'Partitions':<12} {'Messages':<15} {'Avg Bytes/Rec':<15} {'Total Bytes':<15} {'Total Records':<15} {'Status':<15}"
        logging.info(header)
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        
        query_start_time =  datetime.fromisoformat("2025-09-13T00:00:00+00:00")
        query_end_time = datetime.fromisoformat("2025-09-13T23:59:59+00:00")

        # Sort results by topic name
        for result in sorted(results, key=lambda x: x['topic_name']):
            kafka_topic_name = result['topic_name']
            partition_count = result['partition_count']
            total_messages = result.get('total_messages', 0)
            avg_size = result.get('avg_bytes_per_record')

            http_status_code, error_message, bytes_query_result = metrics_client.get_topic_total(KafkaMetric.RECEIVED_BYTES, kafka_cluster_id, kafka_topic_name, query_start_time, query_end_time)
            total_bytes_str = f"{bytes_query_result.get('total', 0):,.0f}" if bytes_query_result else "N/A"

            http_status_code, error_message, records_query_result = metrics_client.get_topic_total(KafkaMetric.RECEIVED_RECORDS, kafka_cluster_id, kafka_topic_name, query_start_time, query_end_time)
            total_records_str = f"{records_query_result.get('total', 0):,.0f}" if records_query_result else "N/A"

            # Format average size
            if avg_size is None:
                avg_size_str = "N/A"
            elif avg_size == 0:
                avg_size_str = "Empty"
            else:
                avg_size_str = f"{avg_size:,.0f}"
            
            # Status
            if 'error' in result:
                status = "Error"
            elif total_messages == 0:
                status = "Empty"
            else:
                status = "Active"
            
            # Format numbers with commas
            messages_str = f"{total_messages:,}"
            logging.info(f"{kafka_topic_name:<40} {partition_count:<12} {messages_str:<15} {avg_size_str:<15} {total_bytes_str:<15} {total_records_str:<15} {status:<15}")

        # Summary statistics
        total_topics = len(results)
        total_partitions = sum(r['partition_count'] for r in results)
        total_messages = sum(r.get('total_messages', 0) for r in results)
        active_topics = len([r for r in results if r.get('total_messages', 0) > 0])

        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("SUMMARY STATISTICS")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Total Topics: {total_topics}")
        logging.info(f"Active Topics: {active_topics} ({active_topics/total_topics*DEFAULT_CHARACTER_REPEAT:.1f}%)")
        logging.info(f"Total Partitions: {total_partitions}")
        logging.info(f"Total Messages: {total_messages:,}")
        logging.info(f"Average Partitions per Topic: {total_partitions/total_topics:.0f}")

        # Export detailed results to JSON
        output_file = 'kafka_topics_analysis.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logging.info(f"\nDetailed results exported to: {output_file}")
        
    except Exception as e:
        logging.error(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()
