from typing import Dict, List, Tuple
from datetime import datetime, timedelta, timezone
import time
import logging
import threading
from confluent_kafka import Consumer, TopicPartition

from utilities import setup_logging
from cc_clients_python_lib.metrics_client import MetricsClient, KafkaMetric, DataMovementType
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_SAMPLING_BATCH_SIZE,
                       DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE,
                       DEFAULT_SAMPLING_MAXIMUM_BATCH_SIZE,
                       DEFAULT_RESTFUL_API_MAX_RETRIES)



__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up module logging
logger = setup_logging()


class ThreadSafeTopicAnalyzer:
    """Thread-safe helper class for analyzing individual topics."""
    
    def __init__(self, admin_client, consumer_config: Dict, kafka_cluster_id: str):
        """"Initialize the ThreadSafeTopicAnalyzer.
        
        Args:
            admin_client: An instance of confluent_kafka.AdminClient for Kafka operations.
            consumer_config (Dict): Configuration dictionary for Kafka consumers.
            kafka_cluster_id (str): The Kafka cluster ID for Metrics API queries.
        """
        self.admin_client = admin_client
        self.consumer_config = consumer_config
        self.kafka_cluster_id = kafka_cluster_id

    def analyze_topic(self, 
                      topic_name: str, 
                      topic_info: Dict, 
                      sampling_batch_size: int,
                      sampling_max_consecutive_nulls: int, 
                      sampling_timeout_seconds: float,
                      sampling_max_continuous_failed_batches: int, 
                      start_time_epoch_ms: int, 
                      iso_start_time: datetime) -> Dict:
        """Analyze a single topic using record sampling.

        Args:
            topic_name (str): The name of the topic to analyze.
            topic_info (Dict): Metadata and retention info about the topic.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            sampling_max_consecutive_nulls (int): Maximum number of consecutive null records to encounter before stopping sampling in a partition.
            sampling_timeout_seconds (float): Timeout in seconds for polling records.
            sampling_max_continuous_failed_batches (int): Maximum number of continuous failed batches before stopping sampling in a partition.
            start_time_epoch_ms (int): The start time in epoch milliseconds for sampling.
            iso_start_time (datetime): The ISO 8601 formatted start time for logging.

        Returns:
            Dict: Analysis results including partition count, total record count, and average bytes per record.
        """
        topic_metadata = topic_info['metadata']
        sampling_days = topic_info['sampling_days_based_on_retention_days']

        logging.info("[Thread-%d] Analyzing topic %s with %d-day rolling window (from %s)",
                     threading.current_thread().ident, topic_name, sampling_days, iso_start_time.isoformat())

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
            logging.info("[Thread-%d] No records available in topic '%s' for sampling.", threading.current_thread().ident, topic_name)
            avg_record_size = 0.0
        else:
            avg_record_size = self.__sample_record_sizes(
                topic_name=topic_name, 
                sampling_batch_size=sampling_batch_size, 
                sampling_max_consecutive_nulls=sampling_max_consecutive_nulls,
                sampling_timeout_seconds=sampling_timeout_seconds,
                sampling_max_continuous_failed_batches=sampling_max_continuous_failed_batches,
                partition_details=partition_details
            )

        # Compile and return the analysis results
        return {
            'topic_name': topic_name,
            'partition_count': partition_count,
            'total_record_count': total_record_count,
            'avg_bytes_per_record': avg_record_size,
            'partition_details': partition_details,
            'is_internal': topic_name.startswith('_')
        }

    def analyze_topic_with_metrics(self, metrics_config: Dict, topic_name: str, topic_info: Dict) -> Dict:
        """Analyze a single topic using Metrics API.

        Args:
            metrics_config (Dict): Configuration dictionary for Metrics API client.
            topic_name (str): The name of the topic to analyze.
            topic_info (Dict): Metadata and retention info about the topic.

        Returns:
            Dict: Analysis results including partition count, compaction status,
                  total record count, and average bytes per record.
        """
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

        # Instantiate the MetricsClient class.
        metrics_client = MetricsClient(metrics_config)

        retry = 0
        max_retries = DEFAULT_RESTFUL_API_MAX_RETRIES
        proceed = False

        while retry < max_retries:
            # Use Metrics API to get the consumer byte consumption
            http_status_code, error_message, rate_limits, bytes_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_BYTES, self.kafka_cluster_id, topic_name)
            if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                retry += 1
                if retry == max_retries:
                    logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'RECEIVED BYTES' metric for topic %s. Max retries reached (%d). Aborting.", threading.current_thread().ident, topic_name, max_retries)
                    result['error'] = "Rate limit exceeded when retrieving 'RECEIVED BYTES' metric."
                    result['total_record_count'] = 0
                    result['avg_bytes_per_record'] = 0.0
                    result['hot_partition_ingress'] = 'no'
                    result['hot_partition_egress'] = 'no'
                    break
                logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'RECEIVED BYTES' metric for topic %s. Retrying %d of %d after %d seconds...", threading.current_thread().ident, topic_name, retry, max_retries, rate_limits['reset_in_seconds'])
                time.sleep(rate_limits['reset_in_seconds'])
                continue
            elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                logging.warning("[Thread-%d] Failed retrieving 'RECEIVED BYTES' metric for topic %s because: %s", threading.current_thread().ident, topic_name, error_message)
                result['error'] = error_message
                result['total_record_count'] = 0
                result['avg_bytes_per_record'] = 0.0
                result['hot_partition_ingress'] = 'no'
                result['hot_partition_egress'] = 'no'
                break
            elif http_status_code == HttpStatus.OK:
                proceed = True
                break

        if proceed:
            proceed = False
            retry = 0

            while retry < max_retries:
                # Use the Confluent Metrics API to get the record count
                http_status_code, error_message, rate_limits, record_query_result = metrics_client.get_topic_daily_aggregated_totals(KafkaMetric.RECEIVED_RECORDS, self.kafka_cluster_id, topic_name) 
                if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                    retry += 1
                    if retry == max_retries:
                        logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric for topic %s. Max retries reached (%d). Aborting.", threading.current_thread().ident, topic_name, max_retries)
                        result['error'] = "Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric."
                        result['total_record_count'] = 0
                        result['avg_bytes_per_record'] = 0.0
                        result['hot_partition_ingress'] = 'no'
                        result['hot_partition_egress'] = 'no'
                        break
                    logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric for topic %s. Retrying %d of %d after %d seconds...", threading.current_thread().ident, topic_name, retry, max_retries, rate_limits['reset_in_seconds'])
                    time.sleep(rate_limits['reset_in_seconds'])
                    continue
                elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                    logging.warning("[Thread-%d] Failed retrieving 'RECEIVED RECORDS' metric for topic %s because: %s", threading.current_thread().ident, topic_name, error_message)
                    result['error'] = error_message
                    result['total_record_count'] = 0
                    result['avg_bytes_per_record'] = 0.0
                    result['hot_partition_ingress'] = 'no'
                    result['hot_partition_egress'] = 'no'
                    break
                elif http_status_code == HttpStatus.OK:
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

                    logging.info("[Thread-%d] Confluent Metrics API - For topic %s, the average bytes per record is %.2f bytes/record for a total of %.0f records.", threading.current_thread().ident, topic_name, avg_bytes_per_record, record_count)

                    proceed = True
                    break

        if proceed and record_count == 0:
            logging.info("[Thread-%d] No records available in topic '%s' for hot partition analysis.", threading.current_thread().ident, topic_name)
            result['hot_partition_ingress'] = 'no'
            result['hot_partition_egress'] = 'no'
            return result
        elif proceed and record_count > 0:
            retry = 0

            while retry < max_retries:
                # Calculate the ISO 8601 formatted start and end times within a rolling window for the last 1 day
                utc_now = datetime.now(timezone.utc)
                one_day_ago = utc_now - timedelta(days=topic_info['sampling_days_based_on_retention_days'])
                iso_start_time = one_day_ago.strftime('%Y-%m-%dT%H:%M:%S')
                iso_end_time = utc_now.strftime('%Y-%m-%dT%H:%M:%S')
                query_start_time =  datetime.fromisoformat(iso_start_time.replace('Z', '+00:00'))
                query_end_time = datetime.fromisoformat(iso_end_time.replace('Z', '+00:00'))

                http_status_code, error_message, rate_limits, is_partition_hot = metrics_client.is_topic_partition_hot(self.kafka_cluster_id, topic_name, DataMovementType.INGRESS, query_start_time, query_end_time)
                if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                    retry += 1
                    if retry == max_retries:
                        logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'HOT_PARTITION_INGRESS' metric for topic %s. Max retries reached (%d). Aborting.", threading.current_thread().ident, topic_name, max_retries)
                        result['error'] = "Rate limit exceeded when retrieving 'HOT_PARTITION_INGRESS' metric."
                        result['total_record_count'] = 0
                        result['avg_bytes_per_record'] = 0.0
                        result['hot_partition_ingress'] = 'no'
                        result['hot_partition_egress'] = 'no'
                        break
                    logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'HOT_PARTITION_INGRESS' metric for topic %s. Retrying %d of %d after %d seconds...", threading.current_thread().ident, topic_name, retry, max_retries, rate_limits['reset_in_seconds'])
                    time.sleep(rate_limits['reset_in_seconds'])
                    continue
                elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                    logging.warning("[Thread-%d] Failed retrieving 'HOT_PARTITION_INGRESS' metric for topic %s because: %s", threading.current_thread().ident, topic_name, error_message)
                    result['error'] = error_message
                    result['total_record_count'] = 0
                    result['avg_bytes_per_record'] = 0.0
                    result['hot_partition_ingress'] = 'no'
                    result['hot_partition_egress'] = 'no'
                    break
                elif http_status_code == HttpStatus.OK:
                    result['hot_partition_ingress'] = 'yes' if is_partition_hot["is_partition_hot"] else 'no'
                    if is_partition_hot["is_partition_hot"]:
                        logging.info("[Thread-%d] Confluent Metrics API - Topic %s is identified as a hot topic by ingress throughput in the last %d days.", threading.current_thread().ident, topic_name, topic_info['sampling_days_based_on_retention_days'])
                    else:
                        logging.info("[Thread-%d] Confluent Metrics API - Topic %s is NOT identified as a hot topic by ingress throughput in the last %d days.", threading.current_thread().ident, topic_name, topic_info['sampling_days_based_on_retention_days'])
                    proceed = True
                    break

            if proceed:
                retry = 0

                while retry < max_retries:
                    http_status_code, error_message, rate_limits, is_partition_hot = metrics_client.is_topic_partition_hot(self.kafka_cluster_id, topic_name, DataMovementType.EGRESS, query_start_time, query_end_time)
                    if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                        retry += 1
                        if retry == max_retries:
                            logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'HOT_PARTITION_EGRESS' metric for topic %s. Max retries reached (%d). Aborting.", threading.current_thread().ident, topic_name, max_retries)
                            result['error'] = "Rate limit exceeded when retrieving 'HOT_PARTITION_EGRESS' metric."
                            result['total_record_count'] = 0
                            result['avg_bytes_per_record'] = 0.0
                            result['hot_partition_ingress'] = 'no'
                            result['hot_partition_egress'] = 'no'
                            break
                        logging.warning("[Thread-%d] Rate limit exceeded when retrieving 'HOT_PARTITION_EGRESS' metric for topic %s. Retrying %d of %d after %d seconds...", threading.current_thread().ident, topic_name, retry, max_retries, rate_limits['reset_in_seconds'])
                        time.sleep(rate_limits['reset_in_seconds'])
                        continue
                    elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                        logging.warning("[Thread-%d] Failed retrieving 'HOT_PARTITION_EGRESS' metric for topic %s because: %s", threading.current_thread().ident, topic_name, error_message)
                        result['error'] = error_message
                        result['total_record_count'] = 0
                        result['avg_bytes_per_record'] = 0.0
                        result['hot_partition_ingress'] = 'no'
                        result['hot_partition_egress'] = 'no'
                        break
                    elif http_status_code == HttpStatus.OK:
                        result['hot_partition_egress'] = 'yes' if is_partition_hot["is_partition_hot"] else 'no'
                        if is_partition_hot["is_partition_hot"]:
                            logging.info("[Thread-%d] Confluent Metrics API - Topic %s is identified as a hot topic by egress throughput in the last %d days.", threading.current_thread().ident, topic_name, topic_info['sampling_days_based_on_retention_days'])
                        else:
                            logging.info("[Thread-%d] Confluent Metrics API - Topic %s is NOT identified as a hot topic by egress throughput in the last %d days.", threading.current_thread().ident, topic_name, topic_info['sampling_days_based_on_retention_days'])
                        break

        return result

    def __get_partition_offsets(self, topic_name: str, partitions: List[int]) -> Dict[int, Tuple[int, int]]:
        """Get low and high watermarks for the topic's partitions.

        Args:
            topic_name (str): The name of the topic.
            partitions (List[int]): List of partition numbers.

        Returns:
            Dict[int, Tuple[int, int]]: Mapping of partition number to (low, high) watermarks.
        """
        # Create unique consumer config for this thread
        thread_consumer_config = {
            **self.consumer_config,
            'group.id': f'watermarks-{int(time.time())}-{threading.current_thread().ident}'
        }
        consumer = Consumer(thread_consumer_config)
    
        try:
            partition_offsets = {}
            topic_partitions = [TopicPartition(topic_name, partition) for partition in partitions]
            
            # Get all watermarks at once for efficiency
            for topic_partition in topic_partitions:
                try:
                    low, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
                    
                    # Validate watermarks
                    if low < 0 or high < 0 or high < low:
                        logging.warning("[Thread-%d] Invalid watermarks for %s[%d]: low=%d, high=%d",
                                        threading.current_thread().ident, topic_name, topic_partition.partition, low, high)
                        partition_offsets[topic_partition.partition] = (0, 0)
                    else:
                        partition_offsets[topic_partition.partition] = (low, high)
                        logging.debug("[Thread-%d] Valid watermarks for %s[%d]: [%d, %d]",
                                      threading.current_thread().ident, topic_name, topic_partition.partition, low, high)

                except Exception as e:
                    logging.warning("[Thread-%d] Failed to get watermarks for %s[%d]: %s",
                                    threading.current_thread().ident, topic_name, topic_partition.partition, e)
                    partition_offsets[topic_partition.partition] = (0, 0)
            
            return partition_offsets
            
        except Exception as e:
            logging.warning("[Thread-%d] Error getting partition offsets for topic %s: %s",
                            threading.current_thread().ident, topic_name, e)
            return {}
        finally:
            consumer.close()

    def __get_record_timestamp_from_offset(self, topic_name: str, partition: int, timestamp_ms: int) -> int:
        """Get the offset of the first record with a timestamp greater than or equal
        to the specified timestamp.

        Args:
            topic_name (str): The name of the topic.
            partition (int): The partition number.
            timestamp_ms (int): The timestamp in milliseconds.

        Returns:
            int: The offset of the first record with a timestamp greater than or equal to the specified timestamp.
        """
        # Create unique consumer config for this thread
        thread_consumer_config = {
            **self.consumer_config,
            'group.id': f'timestamp-{int(time.time())}-{threading.current_thread().ident}-{partition}'
        }
        consumer = Consumer(thread_consumer_config)
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
                    logging.warning("[Thread-%d] Timestamp-based offset %d out of range [%d, %d] for %s[%d]",
                                    threading.current_thread().ident, returned_offset, low, high, topic_name, partition)
                    return max(low, 0)  # Use earliest available offset
            
            return None
        except Exception as e:
            logging.warning("[Thread-%d] Error getting timestamp offset for %s[%d]: %s",
                            threading.current_thread().ident, topic_name, partition, e)
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
            topic_name (str): The name of the topic.
            sampling_batch_size (int): Number of records to process per batch when sampling.
            sampling_max_consecutive_nulls (int): Maximum number of consecutive null records to encounter before stopping sampling in a partition.
            sampling_timeout_seconds (float): Timeout in seconds for polling records.
            sampling_max_continuous_failed_batches (int): Maximum number of continuous failed batches before stopping sampling in a partition.
            partition_details (List[Dict]): List of partition details including partition number,
                                            start and end offsets, and record count.

        Returns:
            float: The average bytes per record sampled.
        """
    
        def adaptive_poll_timeout(consecutive_nulls: int) -> float:
            """Adjust polling timeout based on consecutive nulls encountered.
            
            Args:
                consecutive_nulls (int): The number of consecutive null records encountered.
                
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
                int: Optimal batch size for sampling.
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

            # Create unique consumer config for this thread and partition
            thread_consumer_config = {
                **self.consumer_config,
                'group.id': f'sampler-{int(time.time())}-{threading.current_thread().ident}-{partition_detail["partition_number"]}'
            }
            consumer = Consumer(thread_consumer_config)
            
            try:
                partition_number = partition_detail["partition_number"]
                start_offset = partition_detail["offset_start"]
                end_offset = partition_detail["offset_end"]

                logging.info("[Thread-%d] Partition %03d of %03d: using effective batch size %d (requested: %d, optimal: %d)",
                             threading.current_thread().ident, partition_number, total_partition_count, effective_batch_size, sampling_batch_size, optimal_batch_size)

                # Validate offsets before proceeding
                if start_offset is None or start_offset < 0:
                    logging.warning("[Thread-%d] Invalid start_offset for %s %03d of %03d: %d",
                                    threading.current_thread().ident, topic_name, partition_number, total_partition_count, start_offset)
                    continue
                    
                total_offsets = end_offset - start_offset
                if total_offsets <= 0:
                    continue

                logging.info("[Thread-%d]     Sampling from partition %03d of %03d: offsets [%d, %d)",
                             threading.current_thread().ident, partition_number, total_partition_count, start_offset, end_offset)

                # Setup consumer and validate watermarks
                topic_partition = TopicPartition(topic_name, partition_number)
                consumer.assign([topic_partition])
                
                try:
                    low_watermark, high_watermark = consumer.get_watermark_offsets(topic_partition, timeout=5)
                    
                    # Adjust offsets to be within valid range
                    valid_start = max(low_watermark, start_offset)
                    valid_end = min(high_watermark, end_offset)
                    
                    if valid_start >= valid_end:
                        logging.warning("[Thread-%d] No valid offset range for %s - partition %03d of %03d: adjusted [%d, %d]",
                                        threading.current_thread().ident, topic_name, partition_number, total_partition_count, valid_start, valid_end)
                        continue
                    
                    # Seek to safe start offset
                    topic_partition.offset = valid_start
                    consumer.seek(topic_partition)
                    logging.debug("[Thread-%d] Seeking to offset %d for %s %03d of %03d within valid range [%d, %d]",
                                  threading.current_thread().ident, valid_start, topic_name, partition_number, total_partition_count, low_watermark, high_watermark)

                except Exception as seek_error:
                    logging.warning("[Thread-%d] Failed to seek for %s %03d of %03d: %s",
                                    threading.current_thread().ident, topic_name, partition_number, total_partition_count, seek_error)
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

                    logging.debug("[Thread-%d] Starting batch %d for partition %03d of %03d",
                                  threading.current_thread().ident, batch_count + 1, partition_number, total_partition_count)

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
                                logging.debug("[Thread-%d] Reached end of target range at offset %d",
                                              threading.current_thread().ident, record.offset())
                                break
                            
                            if record.error():
                                logging.warning("[Thread-%d] Consumer error at offset %s: %s",
                                                threading.current_thread().ident, getattr(record, 'offset', 'unknown'), record.error())
                                continue
                            
                            # Verify we're in the correct partition
                            if record.partition() != partition_number:
                                logging.warning("[Thread-%d] Received record from wrong partition: %d != %03d of %03d",
                                                threading.current_thread().ident, record.partition(), partition_number, total_partition_count)
                                continue
                            
                            # Calculate record size
                            try:
                                key_size = len(record.key()) if record.key() else 0
                                value_size = len(record.value()) if record.value() else 0
                                headers_size = sum(len(k) + len(v) for k, v in (record.headers() or []))
                                record_size = key_size + value_size + headers_size
                            except Exception as size_error:
                                logging.warning("[Thread-%d] Error calculating record size at offset %s: %s",
                                                threading.current_thread().ident, getattr(record, 'offset', 'unknown'), size_error)
                                record_size = 0
                            
                            # Update running totals
                            total_size += record_size
                            total_count += 1
                            batch_records_processed += 1
                            partition_record_count += 1
                            
                        except Exception as poll_error:
                            logging.warning("[Thread-%d] Error during polling: %s",
                                            threading.current_thread().ident, poll_error)
                            continue
                    
                    batch_count += 1
                    
                    # Log batch progress
                    if batch_records_processed > 0:
                        failed_batches_in_a_row = 0
                        current_avg = total_size / total_count if total_count > 0 else 0
                        progress_pct = (partition_record_count / max(1, total_partition_offsets)) * 100
                        error_attempts = batch_attempts - batch_records_processed

                        logging.info("[Thread-%d]       Batch %d: %d valid records "
                                     "(%d errors/nulls), progress: %.1f%%, "
                                     "running avg: %.2f bytes",
                                     threading.current_thread().ident, batch_count,
                                     batch_records_processed, error_attempts,
                                     progress_pct, current_avg)
                    else:
                        failed_batches_in_a_row += 1
                        logging.warning("[Thread-%d]       Batch %d: No valid records processed "
                                    "(%d attempts, %d consecutive nulls) "
                                    "[%d of %d consecutive failures]",
                                    threading.current_thread().ident, batch_count,
                                    batch_attempts, consecutive_nulls,
                                    failed_batches_in_a_row, max_failed_batches)

                    # Break if we hit safety limits
                    if consecutive_nulls >= max_consecutive_nulls:
                        logging.warning("[Thread-%d] Too many consecutive null polls (%d) - stopping partition %03d of %03d",
                                        threading.current_thread().ident, consecutive_nulls,
                                        partition_number, total_partition_count)
                        break
                    
                    if batch_attempts >= max_attempts_per_batch and batch_records_processed == 0:
                        logging.warning("[Thread-%d] No progress after %d attempts - stopping partition %03d of %03d",
                                        threading.current_thread().ident, max_attempts_per_batch,
                                        partition_number, total_partition_count)
                        break

                    if failed_batches_in_a_row >= max_failed_batches:
                        logging.warning("[Thread-%d] Giving up on partition %03d of %03d after %d consecutive failed batches",
                                        threading.current_thread().ident, partition_number,
                                        total_partition_count, max_failed_batches)
                        break
            
            except Exception as e:
                logging.warning("[Thread-%d] Error sampling partition %d: %s",
                                threading.current_thread().ident, partition_detail.get('partition_number', 'unknown'), e)
            finally:
                consumer.close()
        
        if total_count > 0:
            avg_size = total_size / total_count
            logging.info("[Thread-%d] Final average: %.2f bytes from %d records",
                         threading.current_thread().ident, avg_size, total_count)
            return avg_size
        else:
            logging.warning("[Thread-%d] No records sampled from topic '%s'",
                            threading.current_thread().ident, topic_name)
            return 0.0