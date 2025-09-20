import csv
from datetime import datetime, timedelta, timezone
import time
from typing import Dict, List, Optional, Tuple
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import Consumer, TopicPartition
import logging

from cc_clients_python_lib.http_status import HttpStatus
from cc_clients_python_lib.metrics_client import MetricsClient, KafkaMetric
from utilities import setup_logging
from constants import (DEFAULT_SAMPLING_DAYS, 
                       DEFAULT_SAMPLING_BATCH_SIZE, 
                       DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR,
                       DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD,
                       DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS,
                       DEFAULT_CHARACTER_REPEAT)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up logging
logger = setup_logging()


class KafkaTopicsAnalyzer:
    """Class to analyze Kafka topics in a cluster."""

    def __init__(self, kafka_cluster_id: str, bootstrap_server_uri: str, kafka_api_key: str, kafka_api_secret: str, metrics_config: Dict):
        """Connect to the Kafka Cluster with the AdminClient.

        Args:
            kafka_cluster_id (string): Your Confluent Cloud Kafka Cluster ID
            bootstrap_server_uri (string): Kafka Cluster URI
            kafka_api_key (string): Your Confluent Cloud Kafka API key
            kafka_api_secret (string): Your Confluent Cloud Kafka API secret
            metrics_config (Dict): Configuration for the MetricsClient
        """
        self.kafka_cluster_id = kafka_cluster_id

        # Instantiate the AdminClient with the provided credentials
        config = {
            'bootstrap.servers': bootstrap_server_uri,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': kafka_api_key,
            'sasl.password': kafka_api_secret,
        }
        self.admin_client = AdminClient(config)
        
        # Setup the Kafka Consumer config for sampling records later
        self.kafka_consumer_config = {
            **config,
            'group.id': f'topics-partition-count-recommender-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000,
            'fetch.min.bytes': 1,
            'enable.metrics.push': False    # Disable metrics pushing for consumers to registered JMX MBeans.  However, is really being set to False to not expose unneccessary noise to the logging output
        }

        # Instantiate the MetricsClient class.
        self.metrics_client = MetricsClient(metrics_config)

    def analyze_all_topics(self, include_internal: bool = False, required_consumption_throughput_factor: float = DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR ,use_sample_records: bool = True, sampling_days: int = DEFAULT_SAMPLING_DAYS, sampling_batch_size: int = DEFAULT_SAMPLING_BATCH_SIZE, topic_filter: str | None = None) -> List[Dict]:
        """Analyze all topics in the cluster.
        
        Args:
            include_internal (bool, optional): Whether to include internal topics. Defaults to False.
            required_consumption_throughput_factor (float, optional): Factor to multiply the consumer throughput to determine required consumption throughput. Defaults to 3.0.
            use_sample_records (bool, optional): Whether to sample records for average size. Defaults to True.
            sampling_days (int, optional): Number of days to look back for sampling. Defaults to 7.
            sampling_batch_size (int, optional): Number of records to process per batch when sampling. Defaults to 1000.
            topic_filter (Optional[str], optional): If provided, only topics containing this string will be analyzed. Defaults to None.
        
        Returns:
            List[Dict]: List of analysis results for each topic.
        """
        logging.info("Connecting to Kafka cluster and fetching metadata...")
        
        # Get cluster metadata
        topics_to_analyze = self.__get_topics_metadata(sampling_days=sampling_days, include_internal=include_internal, topic_filter=topic_filter)
        if not topics_to_analyze:
            return []
        
        logging.info(f"Found {len(topics_to_analyze)} topics to analyze")

        # Analyze topics
        results = []
        report_details = []
        total_recommended_partitions = 0
        report_filename = f"{self.kafka_cluster_id}-recommender-{int(time.time())}-report.csv"
        with open(report_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["topic_name","is_compacted","number_of_records","number_of_partitions","required_throughput","consumer_throughput","recommended_partitions","status"])

        for topic_name, topic_info in topics_to_analyze.items():
            if use_sample_records:
                try:
                    # Calculate the ISO 8601 formatted start timestamp of the rolling window
                    utc_now = datetime.now(timezone.utc)
                    rolling_start = utc_now - timedelta(days=topic_info['sampling_days_based_on_retention_days'])
                    iso_start_time = datetime.fromisoformat(rolling_start.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
                    start_time_epoch_ms = int(rolling_start.timestamp() * 1000)

                    # Analyze the topic
                    result = self.__analyze_topic(topic_name, topic_info, sampling_batch_size, start_time_epoch_ms, iso_start_time)
                    result['is_compacted'] = topic_info['is_compacted']
                    result['sampling_days'] = topic_info['sampling_days_based_on_retention_days']
                    
                except Exception as e:
                    logging.error(f"Failed to analyze topic {topic_name} because {e}")

                    # Add basic info even if analysis fails
                    result = {
                        'topic_name': topic_name,
                        'is_compacted': topic_info['is_compacted'],
                        'sampling_days': topic_info['sampling_days_based_on_retention_days'],
                        'partition_count': len(topic_info['metadata'].partitions),
                        'total_record_count': 0,
                        'avg_bytes_per_record': 0.0,
                        'partition_details': [],
                        'is_internal': topic_name.startswith('_'),
                        'error': str(e)
                    }

                results.append(result)
                
                # Extract necessary details
                partition_count = result['partition_count']
                is_compacted_str = "yes" if result.get('is_compacted', False) else "no"

                # Use sample records to determine throughput
                record_count = result.get('total_record_count', 0)
                consumer_throughput = result.get('avg_bytes_per_record', 0) * record_count
                required_throughput = consumer_throughput * required_consumption_throughput_factor
            else:
                partition_count = len(topic_info['metadata'].partitions)
                is_compacted_str = "yes" if topic_info['is_compacted'] else "no"

                # Use Metrics API to get the consumer byte consumption
                http_status_code, error_message, bytes_query_result = self.metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_BYTES, self.kafka_cluster_id, topic_name)
                if http_status_code != HttpStatus.OK:
                    logging.warning(f"Failed retrieving 'RECEIVED BYTES' metric for topic {topic_name} because the following error occurred: {error_message}")
                    result['error'] = error_message
                    consumer_throughput = 0
                    required_throughput = 0
                    record_count = 0
                else:
                    # Use the Confluent Metrics API to get the record count
                    http_status_code, error_message, record_query_result = self.metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_RECORDS, self.kafka_cluster_id, topic_name)
                    if http_status_code != HttpStatus.OK:
                        logging.warning(f"Failed retrieving 'RECEIVED RECORDS' metric for topic {topic_name} because the following error occurred: {error_message}")
                        result['error'] = error_message
                        record_count = 0
                    else:
                        record_count = record_query_result.get('sum_total', 0)

                        # Calculate daily consumed average bytes per record
                        bytes_daily_totals = bytes_query_result.get('daily_total', [])
                        records_daily_totals = record_query_result.get('daily_total', [])
                        avg_bytes_daily_totals = []

                        for index, record_total in enumerate(records_daily_totals):
                            if record_total > 0:
                                avg_bytes_daily_totals.append(bytes_daily_totals[index]/record_total)

                        avg_bytes_per_record = sum(avg_bytes_daily_totals)/len(avg_bytes_daily_totals) if len(avg_bytes_daily_totals) > 0 else 0

                        logging.info(f"Confluent Metrics API - For topic {topic_name}, the average bytes per record is {avg_bytes_per_record:,.2f} bytes/record for a total of {record_count:,.0f} records.")

                        # Calculate consumer throughput and required throughput
                        consumer_throughput = avg_bytes_per_record * record_count
                        required_throughput = consumer_throughput * required_consumption_throughput_factor

            # Handle cases where record count is zero or an error occurred
            if record_count == 0:
                recommended_partition_count = 0
                status = "error" if 'error' in result else "empty"
            else:
                # Determine recommended partition count
                if required_throughput < DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD:
                    # Set to minimum recommended partitions if below threshold
                    recommended_partition_count = DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS
                else:
                    # Calculate recommended partition count
                    recommended_partition_count = round(required_throughput / consumer_throughput)
                total_recommended_partitions += recommended_partition_count if recommended_partition_count > 0 else 0

                status = "active"

            # Log the results for the topic to a CSV file
            with open(report_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([f"'{topic_name}'", f"'{is_compacted_str}'", record_count, partition_count, required_throughput, consumer_throughput, recommended_partition_count, f"'{status}'"])

        # Summarize results
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("KAFKA TOPICS ANALYSIS RESULTS")
        logging.info(f"Analysis Timestamp: {datetime.now().isoformat()}")
        logging.info(f"Kafka Cluster ID: {self.kafka_cluster_id}")
        logging.info(f"Required Consumption Throughput Factor: {required_consumption_throughput_factor}")

        # Summarize results
        total_topics = len(results)
        total_partitions = sum(result['partition_count'] for result in results)
        total_record_count = sum(result.get('total_record_count', 0) for result in results)

        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("SUMMARY STATISTICS")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Total Topics: {total_topics}")

        active_topics = len([result for result in results if result.get('total_record_count', 0) > 0])
        logging.info(f"Active Topics: {active_topics} ({active_topics/total_topics*100:.1f}%)")

        logging.info(f"Total Partitions: {total_partitions}")
        logging.info(f"Total Recommended Partitions: {total_recommended_partitions}")
        logging.info(f"Total Records: {total_record_count:,}")
        logging.info(f"Average Partitions per Topic: {total_partitions/total_topics:.0f}")
        logging.info(f"Average Recommended Partitions per Topic: {total_recommended_partitions/total_topics:.0f}")

        return report_details

    def __get_topics_metadata(self, sampling_days: int, include_internal: bool, topic_filter: Optional[str]) -> Dict:
        """Get cluster metadata including topics, partitions, and retention.

        Args:
            sampling_days (int): Number of days to look back for sampling.
            include_internal (bool): Whether to include internal topics.
            topic_filter (Optional[str]): If provided, only topics containing this string will be analyzed.
        
        Returns:
            Dict: Metadata of topics in the cluster.
        """
        try:
            # Get all the Kafka Topics' metadata for the Kafka Cluster
            metadata = self.admin_client.list_topics(timeout=60)

            # Now filter the Kafka Topics that are not to analyzed
            topics_to_analyze = {}
            for topic_name, topic_metadata in sorted(metadata.topics.items(), key=lambda x: x[0].lower()):
                # Skip internal topics if not requested
                if not include_internal and topic_name.startswith('_'):
                    continue
                    
                # Apply topic filter if provided
                if topic_filter and topic_filter.lower() not in topic_name.lower():
                    continue

                topics_to_analyze[topic_name] = {"metadata": topic_metadata, 
                                                 "cleanup_policy": None,
                                                 "is_compacted": None,
                                                 "retention_ms": None, 
                                                 "sampling_days_based_on_retention_days": None, 
                                                 "retention_days_for_display": None}

            # Create ConfigResource objects for the topics to be analyzed
            resources = [ConfigResource(ConfigResource.Type.TOPIC, topic_name) for topic_name in topics_to_analyze.keys()]
        
            # Describe configurations for the topics to be analyzed
            configs_result = self.admin_client.describe_configs(resources)

            # Process the results to extract retention.ms for each of the topics to be analyzed
            for resource in resources:
                try:
                    # Get the configuration dictionary for the topic
                    config_dict = configs_result[resource].result(timeout=60)

                    # Extract relevant configurations (cleanup.policy and retention.ms)
                    cleanup_policy = config_dict.get('cleanup.policy')
                    retention_ms = config_dict.get('retention.ms')

                    # Update the topics_to_analyze dictionary with the topic's cleanup policy
                    if cleanup_policy:
                        topics_to_analyze[resource.name]["cleanup_policy"] = cleanup_policy.value
                        topics_to_analyze[resource.name]["is_compacted"] = 'compact' in cleanup_policy.value.lower()
                    else:
                        topics_to_analyze[resource.name]["cleanup_policy"] = "unknown"
                        topics_to_analyze[resource.name]["is_compacted"] = False
                    
                    # Update the topics_to_analyze dictionary with the topic's retention.ms and calculate sampling_days_based_on_retention_days
                    if retention_ms:
                        retention_value = int(retention_ms.value)
                        if retention_value == -1:
                            topics_to_analyze[resource.name]["sampling_days_based_on_retention_days"] = sampling_days
                            topics_to_analyze[resource.name]["retention_days_for_display"] = "Infinite"
                            topics_to_analyze[resource.name]["retention_ms"] = retention_ms
                        else:
                            # (1000 milliseconds * 60 seconds * 60 minutes * 24 hours) = retention in milliseconds / number of milliseconds in a day = number of days
                            number_of_days = retention_value / (1000 * 60 * 60 * 24)
                            topics_to_analyze[resource.name]["sampling_days_based_on_retention_days"] = min(sampling_days, max(1, int(number_of_days)))
                            topics_to_analyze[resource.name]["retention_days_for_display"] = f"{number_of_days:.1f} days"

                        topics_to_analyze[resource.name]["retention_ms"] = retention_ms
                    else:
                        topics_to_analyze[resource.name]["sampling_days_based_on_retention_days"] = sampling_days
                        topics_to_analyze[resource.name]["retention_days_for_display"] = "unknown"
                        topics_to_analyze[resource.name]["retention_ms"] = None
                except:  # noqa: E722
                    # If there's an error retrieving the config, set defaults
                    topics_to_analyze[resource.name]["cleanup_policy"] = "unknown"
                    topics_to_analyze[resource.name]["is_compacted"] = False
                    
                    topics_to_analyze[resource.name]["sampling_days_based_on_retention_days"] = sampling_days
                    topics_to_analyze[resource.name]["retention_days_for_display"] = "unknown"
                    topics_to_analyze[resource.name]["retention_ms"] = None

            return topics_to_analyze
        except Exception as e:
            logger.error(f"Error getting topics metadata: {e}")
            return None
        
    def __get_partition_offsets(self, topic_name: str, partitions: List[int]) -> Dict[int, Tuple[int, int]]:
        """Get low and high watermarks for the topic's partitions.
        
        Args:
            topic_name (str): Topic name.
            partitions (List[int]): List of partition IDs. 

        Returns:
            Dict[int, Tuple[int, int]]: A dictionary with partition IDs as keys and tuples of (low, high) offsets as values.
        """
        consumer = Consumer(self.kafka_consumer_config)
        
        try:
            # Create Topic's TopicPartition objects
            topic_partitions = [TopicPartition(topic_name, partition) for partition in partitions]
            
            # Get high watermarks for all partitions
            partition_offsets = {}
            for topic_partition in topic_partitions:
                try:
                    low, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
                    partition_offsets[topic_partition.partition] = (low, high)
                except Exception as e:
                    logging.error(f"Failed retrieving low and high offsets for {topic_name}'s partition {topic_partition.partition} because {e}")
                    partition_offsets[topic_partition.partition] = (0, 0)
            
            return partition_offsets
            
        except Exception as e:
            logging.error(f"Error getting partition offsets for topic {topic_name}: {e}")
            return {}
        finally:
            consumer.close()

    def __get_record_timestamp_from_offset(self, topic_name: str, partition: int, timestamp_ms: int) -> int:
        """Get the offset of the first record with a timestamp greater than or equal to the specified timestamp.

        Args:
            topic_name (str): Topic name.
            partition (int): Partition number.
            timestamp_ms (int): Timestamp in milliseconds since epoch.

        Returns:
            int: Offset of the record, or None if not found.
        """
        consumer = Consumer(self.kafka_consumer_config)
        topic_partition = TopicPartition(topic_name, partition, timestamp_ms)
        result = consumer.offsets_for_times([topic_partition], timeout=15.0)
        consumer.close()

        if result and result[0].offset != -1:
            return result[0].offset
        return None

    def __sample_record_sizes(self, topic_name: str, sampling_batch_size: int, partition_details: List[Dict]) -> float:
        """Sample record sizes from the specified partitions to calculate average record size.
        
        Args:
            topic_name (str): Topic name to process.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            partition_details (List[Dict]): Details of the partitions to process.

        Returns:
            float: Average record size in bytes, or 0 if no records found.
        """
        total_size = 0
        total_count = 0
        
        for partition_detail in partition_details:
            consumer = Consumer(self.kafka_consumer_config)
            
            try:
                partition_number = partition_detail["partition_number"]
                start_offset = partition_detail["offset_start"]
                end_offset = partition_detail["offset_end"]
                
                total_offsets = end_offset - start_offset
                
                if total_offsets <= 0:
                    continue
                
                logging.info(f"    Streaming {total_offsets:,} records from partition {partition_number}")
                
                # Create TopicPartition and assign it
                topic_partition = TopicPartition(topic_name, partition_number, start_offset)
                consumer.assign([topic_partition])
                consumer.seek(topic_partition)
                
                current_offset = start_offset
                batch_count = 0
                partition_record_count = 0
                
                while current_offset < end_offset:
                    batch_records_processed = 0
                    batch_start_offset = current_offset
                    
                    # Process one batch
                    while batch_records_processed < sampling_batch_size and current_offset < end_offset:
                        record = consumer.poll(timeout=5.0)
                        
                        if record is None:
                            current_offset += 1
                            continue
                        
                        if record.error():
                            current_offset += 1
                            continue
                        
                        # Calculate record size
                        try:
                            key_size = len(record.key()) if record.key() else 0
                            value_size = len(record.value()) if record.value() else 0
                            headers_size = sum(len(k) + len(v) for k, v in (record.headers() or []))
                            record_size = key_size + value_size + headers_size
                        except:  # noqa: E722
                            record_size = 0
                        
                        # Update running totals (streaming approach - saves memory)
                        total_size += record_size
                        total_count += 1
                        
                        current_offset = record.offset() + 1
                        batch_records_processed += 1
                        partition_record_count += 1
                    
                    batch_count += 1
                    
                    # Log progress
                    if batch_records_processed > 0:
                        current_avg = total_size / total_count if total_count > 0 else 0
                        progress_pct = (partition_record_count / total_offsets) * 100
                        logging.info(f"      Streaming batch {batch_count}: {batch_records_processed:,} records "
                                f"(offsets {batch_start_offset:,}-{current_offset-1:,}), "
                                f"progress: {progress_pct:.1f}%, running avg: {current_avg:,.2f} bytes")
            
            except Exception as e:
                logging.error(f"    Error streaming partition {partition_detail.get('partition_number', 'unknown')}: {e}")
            finally:
                consumer.close()
        
        if total_count > 0:
            avg_size = total_size / total_count
            logging.info(f"    Final streaming average: {avg_size:,.2f} bytes from {total_count:,} records")
            return avg_size
        else:
            logging.warning(f"    No records found in topic '{topic_name}'")
            return 0.0

    def __analyze_topic(self, topic_name: str, topic_info: Dict, sampling_batch_size: int, start_time_epoch_ms: int, iso_start_time: datetime) -> Dict:
        """Analyze a single topic.
        
        Args:
            topic_name (str): Name of the topic to analyze.
            topic_info (Dict): Metadata and configuration of the topic.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            start_time_epoch_ms (int): Start time in epoch milliseconds for the rolling window.
            iso_start_time (datetime): Start time as an ISO 8601 formatted datetime object.
            
        Returns:
            Dict: Analysis results including partition count, total messages, average record size, etc.
        """
        topic_metadata = topic_info['metadata']
        sampling_days = topic_info['sampling_days_based_on_retention_days']

        logging.info(f"Analyzing topic {topic_name} with {sampling_days}-day rolling window (from {iso_start_time.isoformat()})")

        partitions = list(topic_metadata.partitions.keys())
        partition_count = len(partitions)
        
        # Get partition offsets to calculate total records
        partition_offsets = self.__get_partition_offsets(topic_name, partitions)
        partition_details = []
        total_record_count = 0
        for partition_number, (low, high) in partition_offsets.items():
            record_count = high - low
            total_record_count += record_count
            
            if record_count > 0:
                # Get the earliest offset in the partition whose record timestamp is >= start_time_epoch_ms
                offset_at_start_time = self.__get_record_timestamp_from_offset(topic_name, partition_number, start_time_epoch_ms)

                if offset_at_start_time is None:
                    offset_at_start_time = high
            else:
                offset_at_start_time = 0
            
            partition_details.append({
                "partition_number": partition_number,
                "offset_start": offset_at_start_time,
                "offset_end": high,
                "record_count": record_count
            })

        if total_record_count == 0:
            logging.info(f"  No records available in topic '{topic_name}' for sampling.")
            avg_record_size = 0.0
        else:
            avg_record_size = avg_record_size = self.__sample_record_sizes(topic_name, sampling_batch_size, partition_details)
        
        # Compile and return the analysis results
        return {
            'topic_name': topic_name,
            'partition_count': partition_count,
            'total_record_count': total_record_count,
            'avg_bytes_per_record': avg_record_size,
            'partition_details': partition_details,
            'is_internal': topic_name.startswith('_')
        }
