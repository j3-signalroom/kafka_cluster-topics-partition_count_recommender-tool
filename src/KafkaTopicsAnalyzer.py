import time
from typing import Dict, List, Tuple
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
import logging

from utilities import setup_logging


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up logging
logger = setup_logging()


class KafkaTopicsAnalyzer:
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
            'fetch.min.bytes': 1
        }

    def analyze_all_topics(self, include_internal: bool = False, use_sample_records: bool = True, sample_size: int = 1000, topic_filter: str | None = None) -> List[Dict]:
        """Analyze all topics in the cluster.
        
        Args:
            include_internal (bool, optional): Whether to include internal topics. Defaults to False.
            use_sample_records (bool, optional): Whether to sample records for average size. Defaults to True.
            sample_size (int, optional): Number of records to sample per topic. Defaults to 1000.
            topic_filter (Optional[str], optional): If provided, only topics containing this string will be analyzed. Defaults to None.
        
        Returns:
            List[Dict]: List of analysis results for each topic.
        """
        logging.info("Connecting to Kafka cluster and fetching metadata...")
        
        # Get cluster metadata
        metadata = self._get_topics_metadata()
        if not metadata:
            return []
        
        # Filter topics
        topics_to_analyze = {}
        for topic_name, topic_metadata in metadata.topics.items():
            # Skip internal topics if not requested
            if not include_internal and topic_name.startswith('_'):
                continue
                
            # Apply topic filter if provided
            if topic_filter and topic_filter.lower() not in topic_name.lower():
                continue
                
            topics_to_analyze[topic_name] = topic_metadata

        logging.info(f"Found {len(topics_to_analyze)} topics to analyze")

        # Analyze topics
        results = []
        for topic_name, topic_metadata in topics_to_analyze.items():
            try:
                result = self._analyze_topic(topic_name, topic_metadata, use_sample_records, sample_size)
                results.append(result)
            except Exception as e:
                logging.error(f"Error analyzing topic {topic_name}: {e}")

                # Add basic info even if analysis fails
                results.append({
                    'topic_name': topic_name,
                    'partition_count': len(topic_metadata.partitions),
                    'total_messages': 0,
                    'avg_bytes_per_record': None,
                    'partition_details': [],
                    'is_internal': topic_name.startswith('_'),
                    'error': str(e)
                })
        
        return results
    
    def _get_topics_metadata(self) -> Dict:
        """Get cluster metadata including topics and partitions.
        
        Returns:
            Dict: Cluster metadata including topics and partitions.
        """
        try:
            metadata = self.admin_client.list_topics(timeout=60)
            return metadata
        except Exception as e:
            logger.error(f"Error getting topics metadata: {e}")
            return None

    def _get_partition_offsets(self, topic: str, partitions: List[int]) -> Dict[int, Tuple[int, int]]:
        """Get low and high watermarks for partitions.
        
        Args:
            topic (str): Topic name.
            partitions (List[int]): List of partition IDs. 

        Returns:
            Dict[int, Tuple[int, int]]: A dictionary with partition IDs as keys and tuples of (low, high) offsets as values.
        """
        consumer = Consumer(self.kafka_consumer_config)
        
        try:
            # Create TopicPartition objects
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            
            # Get high watermarks for all partitions
            partition_offsets = {}
            for tp in topic_partitions:
                try:
                    low, high = consumer.get_watermark_offsets(tp, timeout=10)
                    partition_offsets[tp.partition] = (low, high)
                except Exception as e:
                    logging.error(f"Error getting offsets for {topic} partition {tp.partition}: {e}")
                    partition_offsets[tp.partition] = (0, 0)
            
            return partition_offsets
            
        except Exception as e:
            logging.error(f"Error getting partition offsets for topic {topic}: {e}")
            return {}
        finally:
            consumer.close()

    def _sample_record_sizes(self, topic: str, sample_size: int, timeout_seconds: int = 30) -> float | None:
        """Sample records from a topic to calculate average record size.
        
        Args:
            topic (str): Topic name to sample from.
            sample_size (int): Number of records to sample.
            timeout_seconds (int, optional): Maximum time to wait for sampling. Defaults to 30
        
        Returns:
            float | None: Average record size in bytes, or None if no records found.
        """
        consumer = Consumer(self.kafka_consumer_config)
        
        try:
            # Subscribe to topic
            consumer.subscribe([topic])
            
            record_sizes = []
            start_time = time.time()
            
            logging.info(f"  Sampling {sample_size} records from topic '{topic}'...")
            
            while len(record_sizes) < sample_size and (time.time() - start_time) < timeout_seconds:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    logging.error(f"    Consumer error: {msg.error()}")
                    continue
                
                # Calculate total message size (key + value + headers)
                try:
                    key_size = len(msg.key()) if msg.key() else 0
                except:  # noqa: E722
                    key_size = 0

                try:
                    value_size = len(msg.value()) if msg.value() else 0
                except:  # noqa: E722
                    value_size = 0

                try:
                    headers_size = sum(len(k) + len(v) for k, v in (msg.headers() or []))
                except:  # noqa: E722
                    headers_size = 0

                record_sizes.append(key_size + value_size + headers_size)
            
            if record_sizes:
                avg_size = sum(record_sizes) / len(record_sizes)
                logging.info(f"    Average record size: {avg_size:.2f} bytes (from {len(record_sizes)} samples)")
                return avg_size
            else:
                logging.warning(f"    No records found in topic '{topic}' during sampling period")
                return None
                
        except Exception as e:
            logging.error(f"  Error sampling records from topic {topic}: {e}")
            return None
        finally:
            consumer.close()

    def _analyze_topic(self, topic_name: str, topic_metadata, use_sample_records: bool = True, sample_size: int = 1000) -> Dict:
        """Analyze a single topic.
        
        Args:
            topic_name (str): Name of the topic to analyze.
            topic_metadata: Metadata object for the topic.
            use_sample_records (bool, optional): Whether to sample records for average size. Defaults to True
            
        Returns:
            Dict: Analysis results including partition count, total messages, average record size, etc.
        """
        logging.info(f"\nAnalyzing topic: {topic_name}")
        
        partitions = list(topic_metadata.partitions.keys())
        partition_count = len(partitions)
        
        # Get partition offsets to calculate total messages
        partition_offsets = self._get_partition_offsets(topic_name, partitions)
        
        total_messages = 0
        partition_details = []
        
        for partition_id, (low, high) in partition_offsets.items():
            message_count = high - low
            total_messages += message_count
            
            partition_details.append({
                'partition_id': partition_id,
                'low_watermark': low,
                'high_watermark': high,
                'message_count': message_count
            })
        
        logging.info(f"  Partitions: {partition_count}")
        logging.info(f"  Total messages: {total_messages:,}")
        
        # Sample record sizes if requested and topic has messages
        avg_record_size = None
        if use_sample_records and total_messages > 0:
            avg_record_size = self._sample_record_sizes(topic_name, min(sample_size, total_messages))
        elif total_messages == 0:
            logging.warning(f"  Topic '{topic_name}' is empty - no records to sample")
            avg_record_size = 0.0
        
        return {
            'topic_name': topic_name,
            'partition_count': partition_count,
            'total_messages': total_messages,
            'avg_bytes_per_record': avg_record_size,
            'partition_details': partition_details,
            'is_internal': topic_name.startswith('_')
        }
