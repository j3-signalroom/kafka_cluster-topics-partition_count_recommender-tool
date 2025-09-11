import json
import logging
from dotenv import load_dotenv
import os

from KafkaTopicsAnalyzer import KafkaTopicsAnalyzer


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main():
    load_dotenv()
 
    try:
        # Initialize analyzer
        analyzer = KafkaTopicsAnalyzer(
            bootstrap_server_uri=os.getenv("bootstrap_server_uri"),
            kafka_api_key=os.getenv("kafka_api_key"),
            kafka_api_secret=os.getenv("kafka_api_secret")
        )
        
        # Analyze all topics
        results = analyzer.analyze_all_topics(
            include_internal=os.getenv("INCLUDE_INTERNAL_TOPICS", "False") == "True",
            sample_records=os.getenv("SAMPLE_RECORDS", "True") == "True",
            topic_filter=os.getenv("TOPIC_FILTER")
        )
        
        if not results:
            logging.error("No topics found or analysis failed.")
            return
        
        # Display results
        logging.info("\n" + "=" * 100)
        logging.info("KAFKA TOPICS ANALYSIS RESULTS")
        logging.info("=" * 100)

        # Table header
        header = f"{'Topic Name':<40} {'Partitions':<12} {'Messages':<15} {'Avg Bytes/Rec':<15} {'Status':<15}"
        logging.info(header)
        logging.info("-" * 100)
        
        # Sort results by topic name
        for result in sorted(results, key=lambda x: x['topic_name']):
            topic_name = result['topic_name']
            partition_count = result['partition_count']
            total_messages = result.get('total_messages', 0)
            avg_size = result.get('avg_bytes_per_record')
            
            # Format average size
            if avg_size is None:
                avg_size_str = "N/A"
            elif avg_size == 0:
                avg_size_str = "Empty"
            else:
                avg_size_str = f"{avg_size:.2f}"
            
            # Status
            if 'error' in result:
                status = "Error"
            elif total_messages == 0:
                status = "Empty"
            else:
                status = "Active"
            
            # Format numbers with commas
            messages_str = f"{total_messages:,}"
            
            logging.info(f"{topic_name:<40} {partition_count:<12} {messages_str:<15} {avg_size_str:<15} {status:<15}")
        
        # Summary statistics
        total_topics = len(results)
        total_partitions = sum(r['partition_count'] for r in results)
        total_messages = sum(r.get('total_messages', 0) for r in results)
        active_topics = len([r for r in results if r.get('total_messages', 0) > 0])

        logging.info("\n" + "=" * 100)
        logging.info("SUMMARY STATISTICS")
        logging.info("=" * 100)
        logging.info(f"Total Topics: {total_topics}")
        logging.info(f"Active Topics: {active_topics} ({active_topics/total_topics*100:.1f}%)")
        logging.info(f"Total Partitions: {total_partitions}")
        logging.info(f"Total Messages: {total_messages:,}")
        logging.info(f"Average Partitions per Topic: {total_partitions/total_topics:.2f}")

        # Export detailed results to JSON
        output_file = 'kafka_topics_analysis.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logging.info(f"\nDetailed results exported to: {output_file}")
        
    except Exception as e:
        logging.error(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()
