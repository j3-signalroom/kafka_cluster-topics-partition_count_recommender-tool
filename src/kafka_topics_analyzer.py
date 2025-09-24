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
                       DEFAULT_SAMPLING_TIMEOUT_SECONDS,
                       DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS,
                       DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE,
                       DEFAULT_SAMPLING_MAXIMUM_BATCH_SIZE,
                       DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES,
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
    """Class to analyze Kafka cluster topics."""

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
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'session.timeout.ms': 45000,
            'request.timeout.ms': 30000,
            'fetch.min.bytes': 1,
            'log_level': 3,            
            'enable.partition.eof': True,
            'fetch.message.max.bytes': 1048576,  # 1MB max message size
            'queued.min.messages': 1000,     
            'enable.metrics.push': False         # Disable metrics pushing for consumers to registered JMX MBeans.  However, is really being set to False to not expose unneccessary noise to the logging output
        }

        # Instantiate the MetricsClient class.
        self.metrics_client = MetricsClient(metrics_config)

    def analyze_all_topics(self, 
                           include_internal: bool = False, 
                           required_consumption_throughput_factor: float = DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR,
                           use_sample_records: bool = True, 
                           sampling_days: int = DEFAULT_SAMPLING_DAYS, 
                           sampling_batch_size: int = DEFAULT_SAMPLING_BATCH_SIZE,
                           sampling_max_consecutive_nulls: int = DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS,
                           sampling_timeout_seconds: float = DEFAULT_SAMPLING_TIMEOUT_SECONDS,
                           sampling_max_continuous_failed_batches: int = DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES,
                           topic_filter: str | None = None) -> bool:
        """Analyze all topics in the Kafka cluster.
        
        Args:
            include_internal (bool, optional): Whether to include internal topics. Defaults to False.
            required_consumption_throughput_factor (float, optional): Factor to multiply the consumer throughput to determine required consumption throughput. Defaults to 3.0.
            use_sample_records (bool, optional): Whether to sample records for average size. Defaults to True.
            sampling_days (int, optional): Number of days to look back for sampling. Defaults to 7.
            sampling_batch_size (int, optional): Number of records to process per batch when sampling. Defaults to 10,000.
            sampling_max_consecutive_nulls (int, optional): Maximum number of consecutive null records to encounter before stopping sampling in a partition. Defaults to 50.
            topic_filter (Optional[str], optional): If provided, only topics containing this string will be analyzed. Defaults to None.
        
        Returns:
            bool: True if analysis was successful, False otherwise.
        """
        # Get cluster metadata
        topics_to_analyze = self.__get_topics_metadata(sampling_days=sampling_days, include_internal=include_internal, topic_filter=topic_filter)
        if not topics_to_analyze:
            return []
        
        app_start_time = time.time()

        # Log initial analysis parameters
        self._log_initial_parameters({
            "total_topics_to_analyze": len(topics_to_analyze),
            "include_internal": include_internal,
            "required_consumption_throughput_factor": required_consumption_throughput_factor,
            "topic_filter": topic_filter if topic_filter else "None",
            "use_sample_records": use_sample_records,
            "sampling_days": sampling_days,
            "sampling_batch_size": sampling_batch_size,
            "sampling_max_consecutive_nulls": sampling_max_consecutive_nulls,
            "sampling_timeout_seconds": sampling_timeout_seconds,
            "sampling_max_continuous_failed_batches": sampling_max_continuous_failed_batches
        })

        # Initialize results list and total recommended partitions counter
        results = []
        total_recommended_partitions = 0

        # Prepare CSV report file
        base_filename = f"{self.kafka_cluster_id}-recommender-{int(app_start_time)}"

        # Detail report filename
        report_filename = f"{base_filename}-detail-report.csv"

        # Create the CSV detail report file and write the header row
        with open(report_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["method","topic_name","is_compacted","number_of_records","number_of_partitions","required_throughput","consumer_throughput","recommended_partitions","status"])

        logging.info(f"Created the {report_filename} file")

        # Analyze each topic
        for topic_name, topic_info in topics_to_analyze.items():
            if use_sample_records:
                try:
                    # Calculate the ISO 8601 formatted start timestamp of the rolling window
                    utc_now = datetime.now(timezone.utc)
                    rolling_start = utc_now - timedelta(days=topic_info['sampling_days_based_on_retention_days'])
                    iso_start_time = datetime.fromisoformat(rolling_start.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
                    start_time_epoch_ms = int(rolling_start.timestamp() * 1000)

                    # Analyze the topic
                    result = self.__analyze_topic(topic_name=topic_name, 
                                                  topic_info=topic_info,
                                                  sampling_batch_size=sampling_batch_size,
                                                  sampling_max_consecutive_nulls=sampling_max_consecutive_nulls,
                                                  sampling_timeout_seconds=sampling_timeout_seconds,
                                                  sampling_max_continuous_failed_batches=sampling_max_continuous_failed_batches,
                                                  start_time_epoch_ms=start_time_epoch_ms,
                                                  iso_start_time=iso_start_time)
                    
                    # Add compaction and sampling days info to the result
                    result['is_compacted'] = topic_info['is_compacted']
                    result['sampling_days'] = topic_info['sampling_days_based_on_retention_days']
                    
                except Exception as e:
                    logging.warning(f"Failed to analyze topic {topic_name} because {e}")

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
                
                # Extract the partition count and compaction status
                partition_count = result['partition_count']
                is_compacted_str = "yes" if result.get('is_compacted', False) else "no"

                # Calculate consumer throughput and required throughput
                record_count = result.get('total_record_count', 0)
                consumer_throughput = result.get('avg_bytes_per_record', 0) * record_count
                required_throughput = consumer_throughput * required_consumption_throughput_factor
            else:
                # Extract the partition count and compaction status
                partition_count = len(topic_info['metadata'].partitions)
                is_compacted_str = "yes" if topic_info['is_compacted'] else "no"

                result = {
                    'topic_name': topic_name,
                    'is_compacted': is_compacted_str,
                    'sampling_days': topic_info['sampling_days_based_on_retention_days'],
                    'partition_count': partition_count,
                    'partition_details': topic_info['metadata'].partitions,
                    'is_internal': topic_name.startswith('_')
                }

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
                        result['total_record_count'] = record_count

                        # Calculate daily consumed average bytes per record
                        bytes_daily_totals = bytes_query_result.get('daily_total', [])
                        records_daily_totals = record_query_result.get('daily_total', [])
                        avg_bytes_daily_totals = []

                        # Calculate the average bytes per record for each day where records were consumed
                        for index, record_total in enumerate(records_daily_totals):
                            if record_total > 0:
                                avg_bytes_daily_totals.append(bytes_daily_totals[index]/record_total)

                        # Calculate overall average bytes per record across all days
                        avg_bytes_per_record = sum(avg_bytes_daily_totals)/len(avg_bytes_daily_totals) if len(avg_bytes_daily_totals) > 0 else 0
                        result['avg_bytes_per_record'] = avg_bytes_per_record

                        logging.info(f"Confluent Metrics API - For topic {topic_name}, the average bytes per record is {avg_bytes_per_record:,.2f} bytes/record for a total of {record_count:,.0f} records.")

                        # Calculate consumer throughput and required throughput
                        consumer_throughput = avg_bytes_per_record * record_count
                        required_throughput = consumer_throughput * required_consumption_throughput_factor

            # Append the result to the results list for later summary calculations
            results.append(result)

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

            # Add the topic's calculations to the CSV detail report file.  Note the throughputs are converted to MBs
            with open(report_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([f"{"sampling_records" if use_sample_records else "metrics_api"}", topic_name, is_compacted_str, record_count, partition_count, required_throughput/1024/1024, consumer_throughput/1024/1024, recommended_partition_count, status])

        # Calculate summary statistics
        elapsed_time = time.time() - app_start_time
        overall_topic_count = len(results)
        total_partition_count = sum(result['partition_count'] for result in results)
        total_record_count = sum(result.get('total_record_count', 0) for result in results)
        active_total_partition_count = sum(result.get('partition_count', 0) for result in results if result.get('total_record_count', 0) > 0)
        active_topic_count = len([result for result in results if result.get('total_record_count', 0) > 0])

        if active_total_partition_count > total_recommended_partitions:
            percentage_decrease = (active_total_partition_count - total_recommended_partitions) / active_total_partition_count * 100 if active_total_partition_count > 0 else 0.0
            percentage_increase = 0.0
        else:
            percentage_increase = (total_recommended_partitions - active_total_partition_count) / active_total_partition_count * 100 if active_total_partition_count > 0 else 0.0
            percentage_decrease = 0.0

        # Create the CSV summary report file and write the summary statistics
        self._writer_summary_report(f"{base_filename}-summary-report.csv", {
            "elapsed_time_in_hours": elapsed_time / 3600,
            "use_sample_records": use_sample_records,
            "method": "sampling_records" if use_sample_records else "metrics_api",
            "include_internal": include_internal,
            "overall_topic_count": overall_topic_count,
            "topic_filter": topic_filter if topic_filter else "None",
            "active_topic_count": active_topic_count,
            "active_topic_percentage": (active_topic_count/active_topic_count*100),
            "required_consumption_throughput_factor": required_consumption_throughput_factor,
            "total_partition_count": total_partition_count,
            "total_recommended_partition_count": total_recommended_partitions,
            "active_total_partition_count": active_total_partition_count,
            "percentage_decrease": percentage_decrease,
            "percentage_increase": percentage_increase,
            "total_record_count": total_record_count,
            "average_partitions_per_topic": total_partition_count/overall_topic_count,
            "active_average_partitions_per_topic": total_partition_count/active_topic_count if active_topic_count > 0 else 0,
            "average_recommended_partitions_per_topic": total_recommended_partitions/active_topic_count if active_topic_count > 0 else 0,
        })
            
        # Log summary results
        self._summary_stat_log({
            "elapsed_time_in_hours": elapsed_time / 3600,
            "overall_topic_count": overall_topic_count,
            "active_topic_count": active_topic_count,
            "active_topic_percentage": (active_topic_count/active_topic_count*100),
            "total_partition_count": total_partition_count,
            "total_recommended_partition_count": total_recommended_partitions,
            "active_total_partition_count": active_total_partition_count,
            "total_record_count": total_record_count,
            "average_partitions_per_topic": total_partition_count/overall_topic_count,
            "percentage_decrease": percentage_decrease,
            "percentage_increase": percentage_increase,
            "active_average_partitions_per_topic": total_partition_count/active_topic_count if active_topic_count > 0 else 0,
            "average_recommended_partitions_per_topic": total_recommended_partitions/active_topic_count if active_topic_count > 0 else 0
        })

        return True if len(results) > 0 else False

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
            metadata = self.admin_client.list_topics(timeout=30)

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
                    config_dict = configs_result[resource].result(timeout=30)

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
            partition_offsets = {}
            topic_partitions = [TopicPartition(topic_name, partition) for partition in partitions]
            
            # Get all watermarks at once for efficiency
            for topic_partition in topic_partitions:
                try:
                    low, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
                    
                    # Validate watermarks
                    if low < 0 or high < 0 or high < low:
                        logging.warning(f"Invalid watermarks for {topic_name}[{topic_partition.partition}]: low={low}, high={high}")
                        partition_offsets[topic_partition.partition] = (0, 0)
                    else:
                        partition_offsets[topic_partition.partition] = (low, high)
                        logging.debug(f"Valid watermarks for {topic_name}[{topic_partition.partition}]: [{low}, {high}]")
                        
                except Exception as e:
                    logging.warning(f"Failed to get watermarks for {topic_name}[{topic_partition.partition}]: {e}")
                    partition_offsets[topic_partition.partition] = (0, 0)
            
            return partition_offsets
            
        except Exception as e:
            logging.warning(f"Error getting partition offsets for topic {topic_name}: {e}")
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
        try:
            topic_partition = TopicPartition(topic_name, partition, timestamp_ms)
            result = consumer.offsets_for_times([topic_partition], timeout=15.0)
            
            if result and len(result) > 0 and result[0].offset != -1:
                # Validate the returned offset is within valid range
                low, high = consumer.get_watermark_offsets(TopicPartition(topic_name, partition), timeout=10)
                returned_offset = result[0].offset
                
                if low <= returned_offset <= high:
                    return returned_offset
                else:
                    logging.warning(f"Timestamp-based offset {returned_offset} out of range [{low}, {high}] for {topic_name}[{partition}]")
                    return max(low, 0)  # Use earliest available offset
            
            return None
        except Exception as e:
            logging.warning(f"Error getting timestamp offset for {topic_name}[{partition}]: {e}")
            return None
        finally:
            consumer.close()

    def __sample_record_sizes(self, 
                              topic_name: str, 
                              sampling_batch_size: int, 
                              sampling_max_consecutive_nulls: int, 
                              sampling_timeout_seconds: float,
                              sampling_max_continuous_failed_batches: int,
                              partition_details: List[Dict]) -> float:
        """Sample record sizes from the specified partitions to calculate average record size.
        
        Args:
            topic_name (str): Topic name to process.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            sampling_max_consecutive_nulls (int): Maximum number of consecutive null records to encounter before stopping sampling in a partition.
            sampling_timeout_seconds (float): Maximum time in seconds to spend sampling records per topic.
            sampling_max_continuous_failed_batches (int): Maximum number of continuous failed batches before stopping sampling in a partition.
            partition_details (List[Dict]): Details of the partitions to process.

        Returns:
            float: Average record size in bytes, or 0 if no records found.
        """
        def adaptive_poll_timeout(consecutive_nulls: int) -> float:
            """Adjust polling timeout based on consecutive nulls encountered.

            Args:
                consecutive_nulls (int): Number of consecutive null records encountered.

            Returns:
                float: Adjusted polling timeout in seconds.
            """
            if consecutive_nulls < 5:
                return sampling_timeout_seconds         # Patient at first
            elif consecutive_nulls < 20:
                return sampling_timeout_seconds / 2     # Getting impatient  
            else:
                return sampling_timeout_seconds / 4     # Very impatient, probably no data
            
        def calculate_optimal_batch_size(total_records: int) -> int:
            """Calculate optimal batch size based on partition size.

            Args:
                total_records (int): Total number of records in the partition.

            Returns:
                int: Optimal batch size.
            """
            if total_records < 10000:
                return min(DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE, total_records)
            elif total_records < 1000000:
                return 5000
            elif total_records < 10000000:
                return DEFAULT_SAMPLING_BATCH_SIZE
            else:
                return DEFAULT_SAMPLING_MAXIMUM_BATCH_SIZE

        total_size = 0
        total_count = 0
        total_partition_count = len(partition_details)
        
        for partition_detail in partition_details:
            if partition_detail.get("record_count", 0) <= 0:
                continue

            # Determine effective batch size
            optimal_batch_size = calculate_optimal_batch_size(partition_detail["record_count"])
            effective_batch_size = optimal_batch_size

            consumer = Consumer(self.kafka_consumer_config)
            
            try:
                partition_number = partition_detail["partition_number"]
                start_offset = partition_detail["offset_start"]
                end_offset = partition_detail["offset_end"]

                logging.info(f"Partition {partition_number:03d} of {total_partition_count:03d}: using effective batch size {effective_batch_size:,} (requested: {sampling_batch_size:,}, optimal: {optimal_batch_size:,})")

                # Validate offsets before proceeding
                if start_offset is None or start_offset < 0:
                    logging.warning(f"Invalid start_offset for {topic_name} {partition_number:03d} of {total_partition_count:03d}: {start_offset}")
                    continue
                    
                total_offsets = end_offset - start_offset
                if total_offsets <= 0:
                    continue

                logging.info(f"    Sampling from partition {partition_number:03d} of {total_partition_count:03d}: offsets [{start_offset}, {end_offset})")

                # Setup consumer and validate watermarks
                topic_partition = TopicPartition(topic_name, partition_number)
                consumer.assign([topic_partition])
                
                # Watermarks in Kafka are metadata that indicate the range of available offsets in a partition. So, the
                # code needs to vertify the watermarks before seeking to check that theses boundaries are valid before 
                # attempting to position the consumer at a specific offset
                try:
                    low_watermark, high_watermark = consumer.get_watermark_offsets(topic_partition, timeout=5)
                    
                    # Adjust offsets to be within valid range
                    valid_start = max(low_watermark, start_offset)
                    valid_end = min(high_watermark, end_offset)
                    
                    if valid_start >= valid_end:
                        logging.warning(f"No valid offset range for {topic_name} - partition {partition_number:03d} of {total_partition_count:03d}: adjusted [{valid_start}, {valid_end}]")
                        continue
                    
                    # Seek to safe start offset
                    topic_partition.offset = valid_start
                    consumer.seek(topic_partition)
                    logging.debug(f"Seeking to offset {valid_start} for {topic_name} {partition_number:03d} of {total_partition_count:03d} within valid range [{low_watermark}, {high_watermark}]")
                    
                except Exception as seek_error:
                    # By checking watermarks first, the code ensures that it only seeks to offsets that 
                    # actually exist on the broker, eliminating the previous “Offset out of range” errors.
                    logging.warning(f"Failed to seek for {topic_name} {partition_number:03d} of {total_partition_count:03d}: {seek_error}")
                    continue
                
                # Initialize tracking variables
                batch_count = 0
                partition_record_count = 0
                total_partition_offsets = valid_end - valid_start
                failed_batches_in_a_row = 0
                max_failed_batches = sampling_max_continuous_failed_batches
                
                # Process records in batches
                while partition_record_count < total_partition_offsets:
                    batch_records_processed = 0
                    batch_attempts = 0
                    max_attempts_per_batch = effective_batch_size * 3  # Safety limit
                    max_consecutive_nulls = sampling_max_consecutive_nulls
                    consecutive_nulls = 0
                    
                    logging.debug(f"Starting batch {batch_count + 1} for partition {partition_number:03d} of {total_partition_count:03d}")
                    
                    # Process one batch with safety limits
                    while (batch_records_processed < effective_batch_size and
                        batch_attempts < max_attempts_per_batch and
                        consecutive_nulls < max_consecutive_nulls):
                        
                        batch_attempts += 1
                        
                        try:
                            record = consumer.poll(timeout=adaptive_poll_timeout(consecutive_nulls))
                            
                            if record is None:
                                consecutive_nulls += 1
                                continue
                            
                            consecutive_nulls = 0  # Reset null counter
                            
                            # Check if we've moved beyond our target range
                            if hasattr(record, 'offset') and record.offset() >= valid_end:
                                logging.debug(f"Reached end of target range at offset {record.offset()}")
                                break
                            
                            if record.error():
                                logging.warning(f"Consumer error at offset {getattr(record, 'offset', 'unknown')}: {record.error()}")
                                continue  # Don't count errors toward batch_records_processed
                            
                            # Verify we're in the correct partition
                            if record.partition() != partition_number:
                                logging.warning(f"Received record from wrong partition: {record.partition()} != {partition_number:03d} of {total_partition_count:03d}")
                                continue
                            
                            # Calculate record size
                            try:
                                key_size = len(record.key()) if record.key() else 0
                                value_size = len(record.value()) if record.value() else 0
                                headers_size = sum(len(k) + len(v) for k, v in (record.headers() or []))
                                record_size = key_size + value_size + headers_size
                            except Exception as size_error:
                                logging.warning(f"Error calculating record size at offset {getattr(record, 'offset', 'unknown')}: {size_error}")
                                record_size = 0
                            
                            # Update running totals
                            total_size += record_size
                            total_count += 1
                            batch_records_processed += 1
                            partition_record_count += 1
                            
                        except Exception as poll_error:
                            logging.warning(f"Error during polling: {poll_error}")
                            continue
                    
                    batch_count += 1
                    
                    # Log batch progress
                    if batch_records_processed > 0:
                        failed_batches_in_a_row = 0  # Reset failure counter on success
                        current_avg = total_size / total_count if total_count > 0 else 0
                        progress_pct = (partition_record_count / max(1, total_partition_offsets)) * 100
                        error_attempts = batch_attempts - batch_records_processed
                        
                        logging.info(f"      Batch {batch_count}: {batch_records_processed:,} valid records "
                                    f"({error_attempts} errors/nulls), progress: {progress_pct:.1f}%, "
                                    f"running avg: {current_avg:,.2f} bytes")
                    else:
                        failed_batches_in_a_row += 1
                        logging.warning(f"      Batch {batch_count}: No valid records processed "
                                    f"({batch_attempts} attempts, {consecutive_nulls} consecutive nulls) "
                                    f"[{failed_batches_in_a_row}/{max_failed_batches} consecutive failures]")

                    # Break if we hit safety limits
                    if consecutive_nulls >= max_consecutive_nulls:
                        # Note:  The reason why we are breaking is because the code reached a point where the max 
                        # attempts have been made without receiving new data in the target offset range.  This
                        # could be due to a variety of reasons including that there is no more data to read
                        # in the partition or that there is a gap in the offsets.  In any case, the code
                        # will log a warning and move on to the next partition, because continuing to poll
                        # would be futile.
                        logging.warning(f"Too many consecutive null polls ({consecutive_nulls}) - stopping partition {partition_number:03d} of {total_partition_count:03d}")
                        break
                    
                    # If we made too many attempts without progress, stop processing this partition
                    if batch_attempts >= max_attempts_per_batch and batch_records_processed == 0:
                        logging.warning(f"No progress after {max_attempts_per_batch} attempts - stopping partition {partition_number:03d} of {total_partition_count:03d}")
                        break

                    # If we have too many failed batches in a row, stop processing this partition
                    if failed_batches_in_a_row >= max_failed_batches:
                        logging.warning(f"Giving up on partition {partition_number:03d} of {total_partition_count:03d} after {max_failed_batches} "
                                    f"consecutive failed batches. This partition may have:")
                        logging.warning("  - Corrupted data")
                        logging.warning("  - All records outside the time window")
                        logging.warning("  - Network connectivity issues")
                        logging.warning("  - Broker-side problems")
                        break
            
            except Exception as e:
                logging.warning(f"    Error sampling partition {partition_detail.get('partition_number', 'unknown')}: {e}")
            finally:
                consumer.close()
        
        if total_count > 0:
            avg_size = total_size / total_count
            logging.info(f"    Final average: {avg_size:,.2f} bytes from {total_count:,} records")
            return avg_size
        else:
            logging.warning(f"    No records sampled from topic '{topic_name}'")
            return 0.0

    def __analyze_topic(self, 
                        topic_name: str, 
                        topic_info: Dict, 
                        sampling_batch_size: int, 
                        sampling_max_consecutive_nulls: int,
                        sampling_timeout_seconds: float,
                        sampling_max_continuous_failed_batches: int,
                        start_time_epoch_ms: int, 
                        iso_start_time: datetime) -> Dict:
        """Analyze a single topic.
        
        Args:
            topic_name (str): Name of the topic to analyze.
            topic_info (Dict): Metadata and configuration of the topic.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            sampling_max_consecutive_nulls (int): Maximum number of consecutive null records to encounter before stopping sampling in a partition.
            sampling_timeout_seconds (float): Maximum time in seconds to spend sampling records per topic.
            sampling_max_continuous_failed_batches (int): Maximum number of continuous failed batches before giving up on a partition.
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
            avg_record_size = avg_record_size = self.__sample_record_sizes(topic_name=topic_name, 
                                                                           sampling_batch_size=sampling_batch_size, 
                                                                           sampling_max_consecutive_nulls=sampling_max_consecutive_nulls,
                                                                           sampling_timeout_seconds=sampling_timeout_seconds,
                                                                           sampling_max_continuous_failed_batches=sampling_max_continuous_failed_batches,
                                                                           partition_details=partition_details)

        # Compile and return the analysis results
        return {
            'topic_name': topic_name,
            'partition_count': partition_count,
            'total_record_count': total_record_count,
            'avg_bytes_per_record': avg_record_size,
            'partition_details': partition_details,
            'is_internal': topic_name.startswith('_')
        }
    
    def _log_initial_parameters(self, params: Dict) -> None:
        """Log the initial parameters of the analysis."""
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("INITIAL ANALYSIS PARAMETERS")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Analysis Timestamp: {datetime.now().isoformat()}")
        logging.info(f"Kafka Cluster ID: {self.kafka_cluster_id}")
        logging.info("Connecting to Kafka cluster and retrieving metadata...")
        logging.info(f"Found {params['total_topics_to_analyze']} topics to analyze")
        logging.info(f'{"Including" if params["include_internal"] else "Excluding"} internal topics')
        logging.info(f"Required consumption throughput factor: {params['required_consumption_throughput_factor']:.1f}")
        logging.info(f"Minimum required throughput threshold: {DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD/1024/1024:.1f} MB/s")
        logging.info(f"Topic filter: {params['topic_filter'] if params['topic_filter'] else 'None'}")
        logging.info(f"Default Partition Count: {DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS}")
        logging.info(f'Using {"sample records" if params["use_sample_records"] else "Metrics API"} for average record size calculation')
        if params["use_sample_records"]:
            logging.info(f"Sampling batch size: {params['sampling_batch_size']:,} records")
            logging.info(f"Sampling days: {params['sampling_days']} days")
            logging.info(f"Sampling max consecutive nulls: {params['sampling_max_consecutive_nulls']:,} records")
            logging.info(f"Sampling timeout: {params['sampling_timeout_seconds']:.1f} seconds")
            logging.info(f"Sampling max continuous failed batches: {params['sampling_max_continuous_failed_batches']:,} batches")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)

    def _writer_summary_report(self, report_filename: str, summary_stats: Dict) -> None:
        """Create the CSV summary report file and write the summary statistics.

        Args:
            summary_stats (Dict): Summary statistics to write.
            report_filename (str): Filename for the summary report.
        """
        with open(report_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["stat","value"])
            writer.writerow(["elapsed_time_in_hours", summary_stats['elapsed_time_in_hours']])
            writer.writerow(["method", "sampling_records" if summary_stats['use_sample_records'] else "metrics_api"])
            writer.writerow(["required_consumption_throughput_factor", summary_stats['required_consumption_throughput_factor']])
            writer.writerow(["minimum_required_throughput_threshold", DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD/1024/1024])
            writer.writerow(["default_partition_count", DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS])
            if summary_stats['use_sample_records']:
                writer.writerow(["sampling_batch_size", summary_stats['sampling_batch_size']])
                writer.writerow(["sampling_days", summary_stats['sampling_days']])
                writer.writerow(["sampling_max_consecutive_nulls", summary_stats['sampling_max_consecutive_nulls']])
                writer.writerow(["sampling_timeout", summary_stats['sampling_timeout']])
                writer.writerow(["sampling_max_continuous_failed_batches", summary_stats['sampling_max_continuous_failed_batches']])
            writer.writerow(["total_topics", summary_stats['overall_topic_count']])
            writer.writerow(["internal_topics_included", summary_stats['include_internal']])
            writer.writerow(["topic_filter", summary_stats['topic_filter']])
            writer.writerow(["active_topic_count", summary_stats['active_topic_count']])
            writer.writerow(["active_topic_percentage", (summary_stats['active_topic_count']/summary_stats['active_topic_count']*100)])
            writer.writerow(["total_partition_count", summary_stats['total_partition_count']])
            writer.writerow(["total_recommended_partition_count", summary_stats['total_recommended_partition_count']])
            writer.writerow(["active_total_partition_count", summary_stats['active_total_partition_count']])
            if summary_stats['active_total_partition_count'] > summary_stats['total_recommended_partition_count']:
                writer.writerow(["recommended_percentage_decrease_in_partitions", summary_stats['percentage_decrease']])
            else:
                writer.writerow(["recommended_percentage_increase_in_partitions", summary_stats['percentage_increase']])
            writer.writerow(["total_records", summary_stats['total_record_count']])
            writer.writerow(["average_partitions_per_topic", summary_stats['total_partition_count']/summary_stats['overall_topic_count']])
            writer.writerow(["active_average_partitions_per_topic", summary_stats['total_partition_count']/summary_stats['active_topic_count'] if summary_stats['active_topic_count'] > 0 else 0])
            writer.writerow(["average_recommended_partitions_per_topic", summary_stats['total_recommended_partition_count']/summary_stats['active_topic_count'] if summary_stats['active_topic_count'] > 0 else 0])

    def _summary_stat_log(self, summary_stats: Dict) -> None:
        """Log summary of the analysis."""
        
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("SUMMARY STATISTICS")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Elapsed Time: {summary_stats['elapsed_time_in_hours']:.2f} hours")
        logging.info(f"Total Topics: {summary_stats['overall_topic_count']}")
        logging.info(f"Active Topics: {summary_stats['active_topic_count']}")
        logging.info(f"Active Topics %: {summary_stats['active_topic_count']/summary_stats['overall_topic_count']*100:.1f}%")
        logging.info(f"Total Partitions: {summary_stats['total_partition_count']}")
        logging.info(f"Total Recommended Partitions: {summary_stats['total_recommended_partition_count']}")
        logging.info(f"Non-Empty Topics Total Partitions: {summary_stats['active_total_partition_count']}")
        if summary_stats['active_total_partition_count'] > summary_stats['total_recommended_partition_count']:
            logging.info(f"RECOMMENDED Decrease in Partitions: {summary_stats['percentage_decrease']:.1f}%")
        else:
            logging.info(f"RECOMMENDED Increase in Partitions: {summary_stats['percentage_increase']:.1f}%")
        logging.info(f"Total Records: {summary_stats['total_record_count']:,}")
        logging.info(f"Average Partitions per Topic: {summary_stats['total_partition_count']/summary_stats['overall_topic_count']:.0f}")
        logging.info(f"Average Partitions per Active Topic: {summary_stats['total_partition_count']/summary_stats['active_topic_count']:.0f}")
        logging.info(f"Average Recommended Partitions per Topic: {summary_stats['total_recommended_partition_count']/summary_stats['active_topic_count']:.0f}")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
