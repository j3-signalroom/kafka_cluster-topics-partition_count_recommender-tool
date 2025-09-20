from datetime import datetime, timedelta, timezone
import time
from typing import Dict, List, Optional, Tuple
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import Consumer, TopicPartition
import logging

from utilities import setup_logging
from constants import DEFAULT_SAMPLING_DAYS, DEFAULT_SAMPLING_BATCH_SIZE


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

    def __init__(self, bootstrap_server_uri: str, kafka_api_key: str, kafka_api_secret: str):
        """Connect to the Kafka Cluster with the AdminClient.

        Args:
            bootstrap_server_uri (string): Kafka Cluster URI
            kafka_api_key (string): Your Confluent Cloud Kafka API key
            kafka_api_secret (string): Your Confluent Cloud Kafka API secret
        """
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

    def analyze_all_topics(self, include_internal: bool = False, use_sample_records: bool = True, sampling_days: int = DEFAULT_SAMPLING_DAYS, sampling_batch_size: int = DEFAULT_SAMPLING_BATCH_SIZE, topic_filter: str | None = None) -> List[Dict]:
        """Analyze all topics in the cluster.
        
        Args:
            include_internal (bool, optional): Whether to include internal topics. Defaults to False.
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

        if use_sample_records:
            for topic_name, topic_info in topics_to_analyze.items():
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
                    results.append(result)
                except Exception as e:
                    logging.error(f"Failed to analyze topic {topic_name} because {e}")

                    # Add basic info even if analysis fails
                    results.append({
                        'topic_name': topic_name,
                        'is_compacted': topic_info['is_compacted'],
                        'sampling_days': topic_info['sampling_days_based_on_retention_days'],
                        'partition_count': len(topic_info['metadata'].partitions),
                        'total_record_count': 0,
                        'avg_bytes_per_record': 0.0,
                        'partition_details': [],
                        'is_internal': topic_name.startswith('_'),
                        'error': str(e)
                    })
        else:
            for topic_name, topic_info in topics_to_analyze.items():
                results.append({
                    'topic_name': topic_name,
                    'is_compacted': topic_info['is_compacted'],
                    'sampling_days': topic_info['sampling_days_based_on_retention_days'],
                    'partition_count': len(topic_info['metadata'].partitions),
                    'total_record_count': 0,
                    'avg_bytes_per_record': 0.0,
                    'partition_details': [],
                    'is_internal': topic_name.startswith('_')
                })        
        return results

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
        
        total_record_count = 0
        partition_details = []
        
        for partition_number, (low, high) in partition_offsets.items():
            record_count = high - low
            total_record_count += record_count
                
            # Get the earliest offset in the partition whose record timestamp is >= start_time_epoch_ms
            offset_at_start_time = self.__get_record_timestamp_from_offset(topic_name, partition_number, start_time_epoch_ms)

            if offset_at_start_time is None:
                offset_at_start_time = high
            
            partition_details.append({
                "partition_number": partition_number,
                "offset_start": offset_at_start_time,
                "offset_end": high,
                "record_count": record_count
            })
        
        logging.debug(f"  Partitions: {partition_count}")
        logging.debug(f"  Total record count: {total_record_count:,.0f}")


        # Sample record sizes if requested and topic has messages
        avg_record_size = 0.0
        if total_record_count > 0:
            avg_record_size = self.__sample_record_sizes(topic_name, sampling_batch_size, partition_details)
        elif total_record_count == 0:
            logging.info(f"  No records available in topic '{topic_name}' for sampling.")
            avg_record_size = 0.0
        
        return {
            'topic_name': topic_name,
            'partition_count': partition_count,
            'total_record_count': total_record_count,
            'avg_bytes_per_record': avg_record_size,
            'partition_details': partition_details,
            'is_internal': topic_name.startswith('_')
        }
