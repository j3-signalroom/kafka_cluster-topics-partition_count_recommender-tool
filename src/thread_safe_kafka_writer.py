from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic, ConfigResource
import threading
import json
import logging

from utilities import setup_logging


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Set up module logging
logger = setup_logging()


class ThreadSafeKafkaWriter:
    """Thread-safe Kafka producer for writing analysis results."""

    def __init__(self, 
                 admin_client, 
                 analysis_start_time: int, 
                 kafka_cluster_id: str, 
                 bootstrap_server: str, 
                 topic_name: str, 
                 partition_count: int, 
                 replication_factor: int, 
                 data_retention_in_days: int,
                 sasl_username: str, 
                 sasl_password: str):
        """Initialize the Kafka producer with connection details.

        Args:
            admin_client: An instance of AdminClient to manage Kafka topics.
            analysis_start_time (int): Unique epoch milliseconds for this analysis run.
            kafka_cluster_id (str): The Kafka cluster ID.
            bootstrap_server (str): The Kafka bootstrap server address.
            topic_name (str): The Kafka topic to write to.
            partition_count (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            sasl_username (str): The SASL username for authentication.
            sasl_password (str): The SASL password for authentication.
        """

        # Initialize instance variables
        self.admin_client = admin_client
        self.analysis_start_time = analysis_start_time
        self.kafka_cluster_id = kafka_cluster_id
        self.topic_name = topic_name
        self.lock = threading.Lock()
        self.delivered_count = 0
        self.failed_count = 0

        # Ensure the topic exists or create it
        self.__create_topic_if_not_exists(partition_count=partition_count, replication_factor=replication_factor, data_retention_in_days=data_retention_in_days)

        # Kafka Producer is thread-safe, can be shared across threads
        self.producer = Producer({
            'bootstrap.servers': bootstrap_server,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': sasl_username,
            'sasl.password': sasl_password,
            'linger.ms': 100,           # Batch messages for efficiency
            'compression.type': 'lz4',
            'acks': 'all',              # Wait for all replicas
            'retries': 3
        })
    
    def delivery_callback(self, error_message: str, record) -> None:
        """Callback invoked when a message is delivered or fails.

        Args:
            error_message (str): Error information if delivery failed, else None.
            record: The message that was produced.

        Return(s):
            None
        """
        with self.lock:  # Thread-safe counter updates
            if error_message:
                self.failed_count += 1
                logging.error(f"Message delivery failed: {error_message}")
            else:
                self.delivered_count += 1
                logging.debug(f"Message delivered to {record.topic()}[{record.partition()}]")
    
    def write_result(self, record_value: bytes) -> None:
        """Write analysis result to Kafka topic.

        Args:
            result (Dict): The analysis result to send to Kafka.

        Return(s):
            None
        """
        try:
            key = json.dumps({"analysis_start_time_epoch": self.analysis_start_time, 
                              "kafka_cluster_id": self.kafka_cluster_id, 
                              "topic_name": self.topic_name}).encode('utf-8')
            value = record_value

            # Producer.produce() is thread-safe, no lock needed here
            self.producer.produce(topic=self.topic_name,
                                  key=key,
                                  value=value,
                                  callback=self.delivery_callback)

            # Poll to handle callbacks (thread-safe)
            self.producer.poll(0)

        except BufferError:
            # Queue is full, wait and retry
            logging.warning("Producer queue full, waiting...")
            self.producer.poll(1)  # Wait up to 1 second
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=value,
                callback=self.delivery_callback
            )
        except Exception as e:
            logging.error(f"Failed to produce message: {e}")
    
    def flush_and_close(self, timeout: float = 30.0) -> None:
        """Flush remaining messages and close producer.

        Args:
            timeout (float): Maximum time to wait for messages to be delivered.

        Return(s):
            None
        """
        logging.info("Flushing remaining messages...")
        remaining = self.producer.flush(timeout)
        
        if remaining > 0:
            logging.warning(f"{remaining} messages not delivered before timeout")
        
        logging.info(f"Total delivered: {self.delivered_count}, Failed: {self.failed_count}")

    def __create_topic_if_not_exists(self, partition_count: int, replication_factor: int, data_retention_in_days: int) -> None:
        """Create the results topic if it doesn't exist.

        Args:
            partition_count (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            data_retention_in_days (int): Data retention period in days.
        
        Return(s):
            None
        """
        # Check if topic exists
        topic_list = self.admin_client.list_topics(timeout=10)
        
        # If topic exists, verify retention policy
        retention_policy = '-1' if data_retention_in_days == 0 else str(data_retention_in_days * 24 * 60 * 60 * 1000)  # Convert days to milliseconds
        if self.topic_name in topic_list.topics:
            logging.info(f"Kafka topic '{self.topic_name}' already exists but will verify retention policy")

            # Update existing topic retention policy
            resource = ConfigResource(ConfigResource.Type.TOPIC, self.topic_name)
            resource.set_config('retention.ms', retention_policy)
            self.admin_client.alter_configs([resource])
        else:        
            # Otherwise, create new topic
            logging.info(f"Creating Kafka topic '{self.topic_name}' with {partition_count} partitions")

            new_topic = NewTopic(topic=self.topic_name,
                                num_partitions=partition_count,
                                replication_factor=replication_factor,
                                config={
                                    'cleanup.policy': 'delete',
                                    'retention.ms': retention_policy,
                                    'compression.type': 'lz4'
                                })
            
            futures = self.admin_client.create_topics([new_topic])
            
            # Wait for topic creation
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic is created
                    logging.info(f"Topic '{topic}' created successfully")
                except Exception as e:
                    logging.error(f"Failed to create topic '{topic}': {e}")
                    raise
