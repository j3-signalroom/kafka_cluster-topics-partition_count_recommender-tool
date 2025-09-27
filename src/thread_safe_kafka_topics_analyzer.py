import csv
from datetime import datetime, timedelta, timezone
import time
from typing import Dict, List, Optional
from confluent_kafka.admin import AdminClient, ConfigResource
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from thread_safe_topic_analyzer import ThreadSafeTopicAnalyzer
from thread_safe_csv_writer import ThreadSafeCSVWriter
from cc_clients_python_lib.metrics_client import MetricsClient
from utilities import setup_logging
from constants import (DEFAULT_SAMPLING_DAYS, 
                       DEFAULT_SAMPLING_BATCH_SIZE,
                       DEFAULT_SAMPLING_TIMEOUT_SECONDS,
                       DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS,
                       DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES,
                       DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR,
                       DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD,
                       DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS,
                       DEFAULT_CHARACTER_REPEAT,
                       DEFAULT_MAX_WORKERS_PER_CLUSTER)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up logging
logger = setup_logging()


class ThreadSafeKafkaTopicsAnalyzer:
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

        # Thread-safe progress tracking
        self.progress_lock = threading.Lock()
        self.completed_topics = 0
        self.total_topics = 0

    def analyze_all_topics(self, 
                           use_confluent_cloud_api_key_to_fetch_kafka_credentials: bool = False,
                           include_internal: bool = False, 
                           required_consumption_throughput_factor: float = DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR,
                           use_sample_records: bool = True, 
                           sampling_days: int = DEFAULT_SAMPLING_DAYS, 
                           sampling_batch_size: int = DEFAULT_SAMPLING_BATCH_SIZE,
                           sampling_max_consecutive_nulls: int = DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS,
                           sampling_timeout_seconds: float = DEFAULT_SAMPLING_TIMEOUT_SECONDS,
                           sampling_max_continuous_failed_batches: int = DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES,
                           topic_filter: str | None = None,
                           max_workers: int = DEFAULT_MAX_WORKERS_PER_CLUSTER,
                           min_recommended_partitions: int = DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS,
                           min_consumption_throughput: float = DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD) -> bool:
        """Analyze all topics in the Kafka cluster.
        
        Args:
            use_confluent_cloud_api_key_to_fetch_kafka_credentials (bool, optional): Whether to use Confluent Cloud API key to fetch Kafka credentials. Defaults to False.
            include_internal (bool, optional): Whether to include internal topics. Defaults to False.
            required_consumption_throughput_factor (float, optional): Factor to multiply the consumer throughput to determine required consumption throughput. Defaults to 3.0.
            use_sample_records (bool, optional): Whether to sample records for average size. Defaults to True.
            sampling_days (int, optional): Number of days to look back for sampling. Defaults to 7.
            sampling_batch_size (int, optional): Number of records to process per batch when sampling. Defaults to 10,000.
            sampling_max_consecutive_nulls (int, optional): Maximum number of consecutive null records to encounter before stopping sampling in a partition. Defaults to 50.
            topic_filter (Optional[str], optional): If provided, only topics containing this string will be analyzed. Defaults to None.
            max_workers (int, optional): Maximum number of worker threads for concurrent topic analysis. Defaults to 4.
            min_recommended_partitions (int, optional): The minimum recommended partitions. Defaults to 6.
            min_consumption_throughput (float, optional): The minimum consumption throughput threshold. Defaults to 10 MB/s.
        
        Returns:
            bool: True if analysis was successful, False otherwise.
        """
        # Get cluster metadata
        topics_to_analyze = self.__get_topics_metadata(sampling_days=sampling_days, include_internal=include_internal, topic_filter=topic_filter)
        if not topics_to_analyze:
            return []
        
        app_start_time = time.time()
        self.total_topics = len(topics_to_analyze)

        # Log initial analysis parameters
        self.__log_initial_parameters({
            "use_confluent_cloud_api_key_to_fetch_kafka_credentials": use_confluent_cloud_api_key_to_fetch_kafka_credentials,
            "max_workers": max_workers,
            "total_topics_to_analyze": len(topics_to_analyze),
            "include_internal": include_internal,
            "required_consumption_throughput_factor": required_consumption_throughput_factor,
            "topic_filter": topic_filter if topic_filter else "None",
            "use_sample_records": use_sample_records,
            "sampling_days": sampling_days,
            "sampling_batch_size": sampling_batch_size,
            "sampling_max_consecutive_nulls": sampling_max_consecutive_nulls,
            "sampling_timeout_seconds": sampling_timeout_seconds,
            "sampling_max_continuous_failed_batches": sampling_max_continuous_failed_batches,
            "min_recommended_partitions": min_recommended_partitions,
            "min_consumption_throughput": min_consumption_throughput
        })

        # Initialize results list and total recommended partitions counter
        results = []
        results_lock = threading.Lock()

        # Prepare CSV report file
        base_filename = f"{self.kafka_cluster_id}-recommender-{int(app_start_time)}"

        # Detail report filename
        report_filename = f"{base_filename}-detail-report.csv"

        # Create the CSV detail report file and write the header row
        csv_writer = ThreadSafeCSVWriter(
            report_filename,
            ["method","topic_name","is_compacted","number_of_records","number_of_partitions","required_throughput","consumer_throughput","recommended_partitions","status"]
        )

        logging.info(f"Created the {report_filename} file")

        def update_progress() -> None:
            """Update progress in a thread-safe manner.
            
            Returns:
                None
            """
            with self.progress_lock:
                self.completed_topics += 1
                progress = (self.completed_topics / self.total_topics) * 100
                logging.info(f"Progress: {self.completed_topics}/{self.total_topics} ({progress:.1f}%) topics completed")

        def analyze_topic_worker(topic_name: str, topic_info: Dict) -> Dict:
            """Worker function to analyze a single topic.
            
            Args:
                topic_name (str): Name of the topic to analyze.
                topic_info (Dict): Metadata and configuration of the topic.
                
            Returns:
                Dict: Analysis result for the topic.
            """
            try:
                # Generate unique consumer group ID for each thread
                thread_id = threading.current_thread().ident
                unique_consumer_config = {
                    **self.kafka_consumer_config,
                    'group.id': f'topics-partition-count-recommender-{int(time.time())}-{thread_id}'
                }
                
                # Create a temporary analyzer instance for this thread
                thread_analyzer = ThreadSafeTopicAnalyzer(
                    self.admin_client, 
                    unique_consumer_config, 
                    self.metrics_client,
                    self.kafka_cluster_id
                )

                if use_sample_records:
                    # Calculate the ISO 8601 formatted start timestamp of the rolling window
                    utc_now = datetime.now(timezone.utc)
                    rolling_start = utc_now - timedelta(days=topic_info['sampling_days_based_on_retention_days'])
                    iso_start_time = datetime.fromisoformat(rolling_start.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
                    start_time_epoch_ms = int(rolling_start.timestamp() * 1000)

                    # Analyze the topic
                    result = thread_analyzer.analyze_topic(
                        topic_name=topic_name, 
                        topic_info=topic_info,
                        sampling_batch_size=sampling_batch_size,
                        sampling_max_consecutive_nulls=sampling_max_consecutive_nulls,
                        sampling_timeout_seconds=sampling_timeout_seconds,
                        sampling_max_continuous_failed_batches=sampling_max_continuous_failed_batches,
                        start_time_epoch_ms=start_time_epoch_ms,
                        iso_start_time=iso_start_time
                    )
                    
                    # Add compaction and sampling days info to the result
                    result['is_compacted'] = topic_info['is_compacted']
                    result['sampling_days'] = topic_info['sampling_days_based_on_retention_days']
                    
                else:
                    # Use Metrics API approach
                    result = thread_analyzer.analyze_topic_with_metrics(topic_name, topic_info)
                
                return result
                
            except Exception as e:
                logging.warning(f"Failed to analyze topic {topic_name} because {e}")
                return {
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
            
        # Execute topic analysis in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_topic = {
                executor.submit(analyze_topic_worker, topic_name, topic_info): topic_name
                for topic_name, topic_info in topics_to_analyze.items()
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_topic):
                topic_name = future_to_topic[future]
                try:
                    result = future.result()
                    
                    # Thread-safe result processing
                    with results_lock:
                        results.append(result)
                    
                    # Process result and write to CSV
                    self.__process_and_write_result(
                        result, 
                        min_recommended_partitions,
                        min_consumption_throughput,
                        required_consumption_throughput_factor, 
                        use_sample_records, 
                        csv_writer
                    )
                    
                    update_progress()
                    
                except Exception as e:
                    logging.error(f"Error processing topic {topic_name}: {e}")
                    update_progress()

        # Calculate summary statistics
        summary_stats = self.__calculate_summary_stats(results, 
                                                       time.time() - app_start_time, 
                                                       min_recommended_partitions,
                                                       min_consumption_throughput,
                                                       required_consumption_throughput_factor, 
                                                       use_sample_records, 
                                                       sampling_batch_size, 
                                                       sampling_days, 
                                                       sampling_max_consecutive_nulls, 
                                                       sampling_timeout_seconds, 
                                                       sampling_max_continuous_failed_batches, 
                                                       include_internal, 
                                                       topic_filter)
        
        # Write summary report
        self.__write_summary_report(base_filename, summary_stats)
        
        # Log final results
        self.__log_summary_stats(summary_stats)

        return True if len(results) > 0 else False

    def __process_and_write_result(self, 
                                   result: Dict, 
                                   min_recommended_partitions: int, 
                                   min_consumer_throughput: float, 
                                   required_consumption_throughput_factor: float, 
                                   use_sample_records: bool, 
                                   csv_writer: ThreadSafeCSVWriter) -> None:
        """Process analysis result and write to CSV.

        Args:
            result (Dict): The analysis result for a single topic.
            min_recommended_partitions (int): The minimum recommended partitions.
            min_consumer_throughput (float): The minimum consumption throughput threshold.
            required_consumption_throughput_factor (float): The factor to adjust the required throughput.
            use_sample_records (bool): Whether to use sample records for the analysis.
            csv_writer (ThreadSafeCSVWriter): The CSV writer instance to write the results.

        Return(s):
            None
        """
        topic_name = result['topic_name']
        partition_count = result['partition_count']
        is_compacted_str = "yes" if result.get('is_compacted', False) else "no"
        record_count = result.get('total_record_count', 0)
        
        if record_count > 0:
            avg_bytes_per_record = result.get('avg_bytes_per_record', 0)
            consumer_throughput = avg_bytes_per_record * record_count
            required_throughput = consumer_throughput * required_consumption_throughput_factor
            
            if required_throughput < min_consumer_throughput:
                recommended_partition_count = min_recommended_partitions
            else:
                recommended_partition_count = round(required_throughput / consumer_throughput)
            
            status = "active"
        else:
            consumer_throughput = 0
            required_throughput = 0
            recommended_partition_count = 0
            status = "error" if 'error' in result else "empty"

        # Write to CSV
        method = "sampling_records" if use_sample_records else "metrics_api"
        csv_writer.write_row([
            method, topic_name, is_compacted_str, record_count, partition_count,
            required_throughput/1024/1024, consumer_throughput/1024/1024,
            recommended_partition_count, status
        ])

    def __calculate_summary_stats(self, 
                                 results: List[Dict], 
                                 elapsed_time: float,
                                 min_recommended_partitions: int,
                                 min_consumption_throughput: float,
                                 required_consumption_throughput_factor: float, 
                                 use_sample_records: bool, 
                                 sampling_batch_size: int, 
                                 sampling_days: int, 
                                 sampling_max_consecutive_nulls: int, 
                                 sampling_timeout_seconds: float, 
                                 sampling_max_continuous_failed_batches: int, 
                                 include_internal: bool, 
                                 topic_filter: str) -> Dict:
        """Calculate summary statistics from results.

        Args:
            results (List[Dict]): List of analysis results for all topics.
            elapsed_time (float): Total elapsed time for the analysis in seconds.
            min_recommended_partitions (int): The minimum recommended partitions.
            min_consumption_throughput (float): The minimum consumption throughput threshold.
            required_consumption_throughput_factor (float): The factor to adjust the required throughput.
            use_sample_records (bool): Whether sample records were used for the analysis.
            sampling_batch_size (int): The batch size used for sampling records.
            sampling_days (int): The number of days used for sampling records.
            sampling_max_consecutive_nulls (int): The maximum consecutive nulls allowed during sampling.
            sampling_timeout_seconds (float): The timeout in seconds for sampling records.
            sampling_max_continuous_failed_batches (int): The maximum continuous failed batches allowed during sampling.
            include_internal (bool): Whether internal topics were included in the analysis.
            topic_filter (str): The topic filter applied during analysis.
        
        Return(s):
            Dict: Summary statistics of the analysis.
        """
        overall_topic_count = len(results)
        total_partition_count = sum(result['partition_count'] for result in results)
        total_record_count = sum(result.get('total_record_count', 0) for result in results)
        active_results = [result for result in results if result.get('total_record_count', 0) > 0]
        active_topic_count = len(active_results)
        active_total_partition_count = sum(result.get('partition_count', 0) for result in active_results)
        
        # Calculate total recommended partitions
        total_recommended_partitions = 0
        for result in active_results:
            record_count = result.get('total_record_count', 0)
            if record_count > 0:
                avg_bytes_per_record = result.get('avg_bytes_per_record', 0)
                consumer_throughput = avg_bytes_per_record * record_count
                required_throughput = consumer_throughput * required_consumption_throughput_factor
                
                if required_throughput < min_consumption_throughput:
                    recommended_partition_count = min_recommended_partitions
                else:
                    recommended_partition_count = round(required_throughput / consumer_throughput)
                
                total_recommended_partitions += recommended_partition_count
        
        # Calculate percentage change
        if active_total_partition_count > total_recommended_partitions:
            percentage_decrease = (active_total_partition_count - total_recommended_partitions) / active_total_partition_count * 100 if active_total_partition_count > 0 else 0.0
            percentage_increase = 0.0
        else:
            percentage_increase = (total_recommended_partitions - active_total_partition_count) / active_total_partition_count * 100 if active_total_partition_count > 0 else 0.0
            percentage_decrease = 0.0
        
        return {
            'elapsed_time_hours': elapsed_time / 3600,
            'method': "sampling_records" if use_sample_records else "metrics_api",
            'required_consumption_throughput_factor': required_consumption_throughput_factor,
            'minimum_required_throughput_threshold': min_consumption_throughput/1024/1024,
            'default_partition_count': min_recommended_partitions,
            'sampling_batch_size': sampling_batch_size if use_sample_records else None,
            'sampling_days': sampling_days if use_sample_records else None,
            'sampling_max_consecutive_nulls': sampling_max_consecutive_nulls if use_sample_records else None,
            'sampling_timeout': sampling_timeout_seconds if use_sample_records else None,
            'sampling_max_continuous_failed_batches': sampling_max_continuous_failed_batches if use_sample_records else None,
            'total_topics': overall_topic_count,
            'internal_topics_included': include_internal,
            'topic_filter': topic_filter if topic_filter else "None",
            'active_topic_count': active_topic_count,
            'active_topic_percentage': (active_topic_count/overall_topic_count*100) if overall_topic_count > 0 else 0,
            'total_partitions': total_partition_count,
            'total_recommended_partitions': total_recommended_partitions,
            'active_total_partition_count': active_total_partition_count,
            'percentage_decrease': percentage_decrease,
            'percentage_increase': percentage_increase,
            'total_records': total_record_count,
            'average_partitions_per_topic': total_partition_count/overall_topic_count if overall_topic_count > 0 else 0,
            'active_average_partitions_per_topic': total_partition_count/active_topic_count if active_topic_count > 0 else 0,
            'average_recommended_partitions_per_topic': total_recommended_partitions/active_topic_count if active_topic_count > 0 else 0
        }
    
    def __log_initial_parameters(self, params: Dict) -> None:
        """Log the initial parameters of the analysis.

        Args:
            params (Dict): Dictionary of parameters to log.

        Return(s):
            None
        """
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("INITIAL ANALYSIS PARAMETERS")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Analysis Timestamp: {datetime.now().isoformat()}")
        logging.info(f"Using Confluent Cloud API Key to fetch Kafka credential: {params['use_confluent_cloud_api_key_to_fetch_kafka_credentials']}")
        logging.info(f"Kafka Cluster ID: {self.kafka_cluster_id}")
        logging.info(f"Max worker threads: {params['max_workers']}")
        logging.info("Connecting to Kafka cluster and retrieving metadata...")
        logging.info(f"Found {params['total_topics_to_analyze']} topics to analyze")
        logging.info(f'{"Including" if params["include_internal"] else "Excluding"} internal topics')
        logging.info(f"Required consumption throughput factor: {params['required_consumption_throughput_factor']:.1f}")
        logging.info(f"Minimum required throughput threshold: {params['min_consumption_throughput'] / 1024 / 1024:.1f} MB/s")
        logging.info(f"Topic filter: {params['topic_filter'] if params['topic_filter'] else 'None'}")
        logging.info(f"Default Partition Count: {params['min_recommended_partitions']}")
        logging.info(f'Using {"sample records" if params["use_sample_records"] else "Metrics API"} for average record size calculation')
        if params["use_sample_records"]:
            logging.info(f"Sampling batch size: {params['sampling_batch_size']:,} records")
            logging.info(f"Sampling days: {params['sampling_days']} days")
            logging.info(f"Sampling max consecutive nulls: {params['sampling_max_consecutive_nulls']:,} records")
            logging.info(f"Sampling timeout: {params['sampling_timeout_seconds']:.1f} seconds")
            logging.info(f"Sampling max continuous failed batches: {params['sampling_max_continuous_failed_batches']:,} batches")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)

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
        
    def __write_summary_report(self, base_filename: str, stats: Dict) -> None:
        """Write summary statistics to a CSV file.
        
        Args:
            base_filename (str): Base filename for the report.
            stats (Dict): Summary statistics to write.
            
        Return(s):
            None
        """
        report_filename = f"{base_filename}-summary-report.csv"
        
        with open(report_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["stat","value"])
            
            for key, value in stats.items():
                if value is not None:
                    writer.writerow([key, value])

    def __log_summary_stats(self, stats: Dict) -> None:
        """Log summary statistics to console and file.
        
        Args:
            stats (Dict): Summary statistics to log.
            
        Returns:
            None
        """
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
        logging.info("ANALYSIS SUMMARY STATISTICS")
        logging.info("-" * DEFAULT_CHARACTER_REPEAT)
        logging.info(f"Elapsed Time: {stats['elapsed_time_hours']:.2f} hours")
        logging.info(f"Total Topics: {stats['total_topics']}")
        logging.info(f"Active Topics: {stats['active_topic_count']}")
        logging.info(f"Active Topics %: {stats['active_topic_percentage']:.1f}%")
        logging.info(f"Total Partitions: {stats['total_partitions']}")
        logging.info(f"Total Recommended Partitions: {stats['total_recommended_partitions']}")
        logging.info(f"Non-Empty Topics Total Partitions: {stats['active_total_partition_count']}")
        
        if stats['percentage_decrease'] > 0:
            logging.info(f"RECOMMENDED Decrease in Partitions: {stats['percentage_decrease']:.1f}%")
        elif stats['percentage_increase'] > 0:
            logging.info(f"RECOMMENDED Increase in Partitions: {stats['percentage_increase']:.1f}%")
            
        logging.info(f"Total Records: {stats['total_records']:,}")
        logging.info(f"Average Partitions per Topic: {stats['average_partitions_per_topic']:.0f}")
        logging.info(f"Average Partitions per Active Topic: {stats['active_average_partitions_per_topic']:.0f}")
        logging.info(f"Average Recommended Partitions per Topic: {stats['average_recommended_partitions_per_topic']:.0f}")
        logging.info("=" * DEFAULT_CHARACTER_REPEAT)
