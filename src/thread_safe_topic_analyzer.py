from typing import Dict, List, Tuple
from datetime import datetime
import time
import logging
import threading
from confluent_kafka import Consumer, TopicPartition

from utilities import setup_logging
from cc_clients_python_lib.metrics_client import KafkaMetric
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_SAMPLING_BATCH_SIZE,
                       DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE,
                       DEFAULT_SAMPLING_MAXIMUM_BATCH_SIZE)



__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up logging
logger = setup_logging()


class ThreadSafeTopicAnalyzer:
    """Thread-safe helper class for analyzing individual topics."""
    
    def __init__(self, admin_client, consumer_config: Dict, metrics_client, kafka_cluster_id: str):
        self.admin_client = admin_client
        self.consumer_config = consumer_config
        self.metrics_client = metrics_client
        self.kafka_cluster_id = kafka_cluster_id

    def analyze_topic(self, topic_name: str, topic_info: Dict, sampling_batch_size: int, 
                     sampling_max_consecutive_nulls: int, sampling_timeout_seconds: float,
                     sampling_max_continuous_failed_batches: int, start_time_epoch_ms: int, 
                     iso_start_time: datetime) -> Dict:
        """Analyze a single topic using record sampling."""
        topic_metadata = topic_info['metadata']
        sampling_days = topic_info['sampling_days_based_on_retention_days']

        logging.info(f"[Thread-{threading.current_thread().ident}] Analyzing topic {topic_name} with {sampling_days}-day rolling window (from {iso_start_time.isoformat()})")

        partitions = list(topic_metadata.partitions.keys())
        partition_count = len(partitions)
        
        # Get partition offsets to calculate total records
        partition_offsets = self._get_partition_offsets(topic_name, partitions)
        partition_details = []
        total_record_count = 0
        
        for partition_number, (low, high) in partition_offsets.items():
            record_count = high - low
            total_record_count += record_count
            
            if record_count > 0:
                # Get the earliest offset in the partition whose record timestamp is >= start_time_epoch_ms
                offset_at_start_time = self._get_record_timestamp_from_offset(topic_name, partition_number, start_time_epoch_ms)
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
            logging.info(f"[Thread-{threading.current_thread().ident}] No records available in topic '{topic_name}' for sampling.")
            avg_record_size = 0.0
        else:
            avg_record_size = self._sample_record_sizes(
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

    def analyze_topic_with_metrics(self, topic_name: str, topic_info: Dict) -> Dict:
        """Analyze a single topic using Metrics API."""
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

        bytes_retry = 0
        max_bytes_retries = 3
        retry_delay_in_seconds = 60
        proceed_to_records = False

        while bytes_retry < max_bytes_retries:
            # Use Metrics API to get the consumer byte consumption
            http_status_code, error_message, bytes_query_result = self.metrics_client.get_topic_daily_aggregated_totals(
                KafkaMetric.RECEIVED_BYTES, self.kafka_cluster_id, topic_name
            )
            if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                bytes_retry += 1
                if bytes_retry == max_bytes_retries:
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Rate limit exceeded when retrieving 'RECEIVED BYTES' metric for topic {topic_name}. Max retries reached ({max_bytes_retries}). Aborting.")
                    result['error'] = "Rate limit exceeded when retrieving 'RECEIVED BYTES' metric."
                    result['total_record_count'] = 0
                    result['avg_bytes_per_record'] = 0.0
                    break
                logging.warning(f"[Thread-{threading.current_thread().ident}] Rate limit exceeded when retrieving 'RECEIVED BYTES' metric for topic {topic_name}. Retrying {bytes_retry}/{max_bytes_retries} after {retry_delay_in_seconds} seconds...")
                time.sleep(retry_delay_in_seconds)
                continue
            elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                logging.warning(f"[Thread-{threading.current_thread().ident}] Failed retrieving 'RECEIVED BYTES' metric for topic {topic_name} because: {error_message}")
                result['error'] = error_message
                result['total_record_count'] = 0
                result['avg_bytes_per_record'] = 0.0
                break
            elif http_status_code == HttpStatus.OK:
                proceed_to_records = True
                break

        if proceed_to_records:
            records_retry = 0
            max_records_retries = 3

            while records_retry < max_records_retries:
                # Use the Confluent Metrics API to get the record count
                http_status_code, error_message, record_query_result = self.metrics_client.get_topic_daily_aggregated_totals(
                    KafkaMetric.RECEIVED_RECORDS, self.kafka_cluster_id, topic_name
                )
                if http_status_code == HttpStatus.RATE_LIMIT_EXCEEDED:
                    records_retry += 1
                    if records_retry == max_records_retries:
                        logging.warning(f"[Thread-{threading.current_thread().ident}] Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric for topic {topic_name}. Max retries reached ({max_records_retries}). Aborting.")
                        result['error'] = "Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric."
                        result['total_record_count'] = 0
                        result['avg_bytes_per_record'] = 0.0
                        break
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Rate limit exceeded when retrieving 'RECEIVED RECORDS' metric for topic {topic_name}. Retrying {records_retry}/{max_records_retries} after {retry_delay_in_seconds} seconds...")
                    time.sleep(retry_delay_in_seconds)
                    continue
                elif http_status_code not in (HttpStatus.OK, HttpStatus.RATE_LIMIT_EXCEEDED):
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Failed retrieving 'RECEIVED RECORDS' metric for topic {topic_name} because: {error_message}")
                    result['error'] = error_message
                    result['total_record_count'] = 0
                    result['avg_bytes_per_record'] = 0.0
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

                    logging.info(f"[Thread-{threading.current_thread().ident}] Confluent Metrics API - For topic {topic_name}, the average bytes per record is {avg_bytes_per_record:,.2f} bytes/record for a total of {record_count:,.0f} records.")

                    break

        return result

    def _get_partition_offsets(self, topic_name: str, partitions: List[int]) -> Dict[int, Tuple[int, int]]:
        """Get low and high watermarks for the topic's partitions."""
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
                        logging.warning(f"[Thread-{threading.current_thread().ident}] Invalid watermarks for {topic_name}[{topic_partition.partition}]: low={low}, high={high}")
                        partition_offsets[topic_partition.partition] = (0, 0)
                    else:
                        partition_offsets[topic_partition.partition] = (low, high)
                        logging.debug(f"[Thread-{threading.current_thread().ident}] Valid watermarks for {topic_name}[{topic_partition.partition}]: [{low}, {high}]")
                        
                except Exception as e:
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Failed to get watermarks for {topic_name}[{topic_partition.partition}]: {e}")
                    partition_offsets[topic_partition.partition] = (0, 0)
            
            return partition_offsets
            
        except Exception as e:
            logging.warning(f"[Thread-{threading.current_thread().ident}] Error getting partition offsets for topic {topic_name}: {e}")
            return {}
        finally:
            consumer.close()

    def _get_record_timestamp_from_offset(self, topic_name: str, partition: int, timestamp_ms: int) -> int:
        """Get the offset of the first record with a timestamp greater than or equal to the specified timestamp."""
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
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Timestamp-based offset {returned_offset} out of range [{low}, {high}] for {topic_name}[{partition}]")
                    return max(low, 0)  # Use earliest available offset
            
            return None
        except Exception as e:
            logging.warning(f"[Thread-{threading.current_thread().ident}] Error getting timestamp offset for {topic_name}[{partition}]: {e}")
            return None
        finally:
            consumer.close()

    def _sample_record_sizes(self, topic_name: str, sampling_batch_size: int, 
                           sampling_max_consecutive_nulls: int, sampling_timeout_seconds: float,
                           sampling_max_continuous_failed_batches: int,
                           partition_details: List[Dict]) -> float:
        """Sample record sizes from the specified partitions to calculate average record size."""
        
        def adaptive_poll_timeout(consecutive_nulls: int) -> float:
            """Adjust polling timeout based on consecutive nulls encountered."""
            if consecutive_nulls < 5:
                return sampling_timeout_seconds         # Patient at first
            elif consecutive_nulls < 20:
                return sampling_timeout_seconds / 2     # Getting impatient  
            else:
                return sampling_timeout_seconds / 4     # Very impatient, probably no data
            
        def calculate_optimal_batch_size(total_records: int) -> int:
            """Calculate optimal batch size based on partition size."""
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

                logging.info(f"[Thread-{threading.current_thread().ident}] Partition {partition_number:03d} of {total_partition_count:03d}: using effective batch size {effective_batch_size:,} (requested: {sampling_batch_size:,}, optimal: {optimal_batch_size:,})")

                # Validate offsets before proceeding
                if start_offset is None or start_offset < 0:
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Invalid start_offset for {topic_name} {partition_number:03d} of {total_partition_count:03d}: {start_offset}")
                    continue
                    
                total_offsets = end_offset - start_offset
                if total_offsets <= 0:
                    continue

                logging.info(f"[Thread-{threading.current_thread().ident}]     Sampling from partition {partition_number:03d} of {total_partition_count:03d}: offsets [{start_offset}, {end_offset})")

                # Setup consumer and validate watermarks
                topic_partition = TopicPartition(topic_name, partition_number)
                consumer.assign([topic_partition])
                
                try:
                    low_watermark, high_watermark = consumer.get_watermark_offsets(topic_partition, timeout=5)
                    
                    # Adjust offsets to be within valid range
                    valid_start = max(low_watermark, start_offset)
                    valid_end = min(high_watermark, end_offset)
                    
                    if valid_start >= valid_end:
                        logging.warning(f"[Thread-{threading.current_thread().ident}] No valid offset range for {topic_name} - partition {partition_number:03d} of {total_partition_count:03d}: adjusted [{valid_start}, {valid_end}]")
                        continue
                    
                    # Seek to safe start offset
                    topic_partition.offset = valid_start
                    consumer.seek(topic_partition)
                    logging.debug(f"[Thread-{threading.current_thread().ident}] Seeking to offset {valid_start} for {topic_name} {partition_number:03d} of {total_partition_count:03d} within valid range [{low_watermark}, {high_watermark}]")
                    
                except Exception as seek_error:
                    logging.warning(f"[Thread-{threading.current_thread().ident}] Failed to seek for {topic_name} {partition_number:03d} of {total_partition_count:03d}: {seek_error}")
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
                    
                    logging.debug(f"[Thread-{threading.current_thread().ident}] Starting batch {batch_count + 1} for partition {partition_number:03d} of {total_partition_count:03d}")
                    
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
                                logging.debug(f"[Thread-{threading.current_thread().ident}] Reached end of target range at offset {record.offset()}")
                                break
                            
                            if record.error():
                                logging.warning(f"[Thread-{threading.current_thread().ident}] Consumer error at offset {getattr(record, 'offset', 'unknown')}: {record.error()}")
                                continue
                            
                            # Verify we're in the correct partition
                            if record.partition() != partition_number:
                                logging.warning(f"[Thread-{threading.current_thread().ident}] Received record from wrong partition: {record.partition()} != {partition_number:03d} of {total_partition_count:03d}")
                                continue
                            
                            # Calculate record size
                            try:
                                key_size = len(record.key()) if record.key() else 0
                                value_size = len(record.value()) if record.value() else 0
                                headers_size = sum(len(k) + len(v) for k, v in (record.headers() or []))
                                record_size = key_size + value_size + headers_size
                            except Exception as size_error:
                                logging.warning(f"[Thread-{threading.current_thread().ident}] Error calculating record size at offset {getattr(record, 'offset', 'unknown')}: {size_error}")
                                record_size = 0
                            
                            # Update running totals
                            total_size += record_size
                            total_count += 1
                            batch_records_processed += 1
                            partition_record_count += 1
                            
                        except Exception as poll_error:
                            logging.warning(f"[Thread-{threading.current_thread().ident}] Error during polling: {poll_error}")
                            continue
                    
                    batch_count += 1
                    
                    # Log batch progress
                    if batch_records_processed > 0:
                        failed_batches_in_a_row = 0
                        current_avg = total_size / total_count if total_count > 0 else 0
                        progress_pct = (partition_record_count / max(1, total_partition_offsets)) * 100
                        error_attempts = batch_attempts - batch_records_processed
                        
                        logging.info(f"[Thread-{threading.current_thread().ident}]       Batch {batch_count}: {batch_records_processed:,} valid records "
                                    f"({error_attempts} errors/nulls), progress: {progress_pct:.1f}%, "
                                    f"running avg: {current_avg:,.2f} bytes")
                    else:
                        failed_batches_in_a_row += 1
                        logging.warning(f"[Thread-{threading.current_thread().ident}]       Batch {batch_count}: No valid records processed "
                                    f"({batch_attempts} attempts, {consecutive_nulls} consecutive nulls) "
                                    f"[{failed_batches_in_a_row}/{max_failed_batches} consecutive failures]")

                    # Break if we hit safety limits
                    if consecutive_nulls >= max_consecutive_nulls:
                        logging.warning(f"[Thread-{threading.current_thread().ident}] Too many consecutive null polls ({consecutive_nulls}) - stopping partition {partition_number:03d} of {total_partition_count:03d}")
                        break
                    
                    if batch_attempts >= max_attempts_per_batch and batch_records_processed == 0:
                        logging.warning(f"[Thread-{threading.current_thread().ident}] No progress after {max_attempts_per_batch} attempts - stopping partition {partition_number:03d} of {total_partition_count:03d}")
                        break

                    if failed_batches_in_a_row >= max_failed_batches:
                        logging.warning(f"[Thread-{threading.current_thread().ident}] Giving up on partition {partition_number:03d} of {total_partition_count:03d} after {max_failed_batches} consecutive failed batches")
                        break
            
            except Exception as e:
                logging.warning(f"[Thread-{threading.current_thread().ident}] Error sampling partition {partition_detail.get('partition_number', 'unknown')}: {e}")
            finally:
                consumer.close()
        
        if total_count > 0:
            avg_size = total_size / total_count
            logging.info(f"[Thread-{threading.current_thread().ident}] Final average: {avg_size:,.2f} bytes from {total_count:,} records")
            return avg_size
        else:
            logging.warning(f"[Thread-{threading.current_thread().ident}] No records sampled from topic '{topic_name}'")
            return 0.0