from typing import Final
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import NewTopic, ConfigResource
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import threading
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


KAFKA_TOPIC_KEY_SCHEMA: Final[str] = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://signalroom.ai/kafka-metrics-key.schema.json",
  "title": "Kafka Metrics API Key",
  "description": "Schema for Kafka topic metrics key",
  "type": "object",
  "properties": {
    "analysis_start_time_epoch": {
      "type": "integer",
      "minimum": 0,
      "description": "Unique epoch milliseconds for this analysis run",
      "examples": [1700000000000]
    },
    "kafka_cluster_id": {
      "type": "string",
      "description": "ID of the Kafka cluster",
      "examples": ["lkc-abc123"]
    },
    "topic_name": {
      "type": "string",
      "description": "Name of the Kafka topic",
      "examples": ["example-kafka-topic"]
    }
  },
  "required": [
    "analysis_start_time_epoch",
    "kafka_cluster_id",
    "topic_name"
  ],
  "additionalProperties": false
}
"""

KAFKA_TOPIC_VALUE_SCHEMA: Final[str] = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://signalroom.ai/kafka-metrics-value.schema.json",
  "title": "Kafka Metrics API Response",
  "description": "Schema for Kafka topic metrics and partition recommendations",
  "type": "object",
  "properties": {
    "method": {
      "type": "string",
      "enum": ["sampling_records", "metrics_api"],
      "description": "The API method used to retrieve metrics"
    },
    "topic_name": {
      "type": "string",
      "description": "Name of the Kafka topic",
      "examples": ["example-kafka-topic"]
    },
    "is_compacted": {
      "type": "string",
      "enum": ["yes", "no"],
      "description": "Indicates whether the topic uses log compaction"
    },
    "number_of_records": {
      "type": "integer",
      "minimum": 0,
      "description": "Total number of records in the topic"
    },
    "number_of_partitions": {
      "type": "integer",
      "minimum": 0,
      "description": "Current number of partitions for the topic"
    },
    "required_throughput": {
      "type": "number",
      "minimum": 0,
      "description": "Required throughput in bytes per second or messages per second"
    },
    "consumer_throughput": {
      "type": "number",
      "minimum": 0,
      "description": "Consumer throughput in bytes per second or messages per second"
    },
    "recommended_partitions": {
      "type": "integer",
      "minimum": 0,
      "description": "Recommended number of partitions based on throughput analysis"
    },
    "hot_partition_ingress": {
      "type": "string",
      "enum": ["yes", "no", "n/a"],
      "description": "Indicates whether hot partitions are detected on ingress"
    },
    "hot_partition_egress": {
      "type": "string",
      "enum": ["yes", "no", "n/a"],
      "description": "Indicates whether hot partitions are detected on egress"
    },
    "status": {
      "type": "string",
      "enum": ["active", "empty", "error"],
      "description": "Current status of the topic"
    }
  },
  "required": [
    "method",
    "topic_name",
    "is_compacted",
    "number_of_records",
    "number_of_partitions",
    "required_throughput",
    "consumer_throughput",
    "recommended_partitions",
    "hot_partition_ingress",
    "hot_partition_egress",
    "status"
  ],
  "additionalProperties": false
}
"""


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
                 sasl_password: str,
                 sr_url: str,
                 sr_api_key_secret: str) -> None:
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
            sr_url (str): The Schema Registry URL.
            sr_api_key_secret (str): The Schema Registry API key and secret in the format "key:secret".
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

        try:
            # Initialize Schema Registry client and JSONSerializers
            self.sr_client = SchemaRegistryClient({
                "url": sr_url,
                "basic.auth.user.info": sr_api_key_secret
            })
            
        except Exception as e:
            logging.error(f"Failed to create Schema Registry client or serializers: {e}")
            raise

        self.json_key_serializer = JSONSerializer(KAFKA_TOPIC_KEY_SCHEMA, self.sr_client, self.key_to_dict)
        self.json_value_serializer = JSONSerializer(KAFKA_TOPIC_VALUE_SCHEMA, self.sr_client, self.value_to_dict)

        # Kafka Producer is thread-safe, can be shared across threads
        try:
            self.producer = Producer({
                "bootstrap.servers": bootstrap_server,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": sasl_username,
                "sasl.password": sasl_password,
                "linger.ms": 100,               # Batch messages for efficiency
                "compression.type": "lz4",
                "acks": "all",                  # Wait for all replicas
                "retries": 10,
                'retry.backoff.ms': 100,
                'delivery.timeout.ms': 300000,  # Delivery timeout 5 minutes
                'enable.idempotence': True,     # Prevents duplicate messages on retry
            })
        except Exception as e:
            logging.error(f"Failed to create Kafka producer: {e}")
            raise
    
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
    
    def key_to_dict(self, key, ctx):
      """Returns a dict representation of a key instance for serialization.

      Args:
          key: Entity instance.
          ctx (SerializationContext): Metadata pertaining to the serialization
              operation.

      Returns:
          dict: Dict populated with entity attributes to be serialized.
      """
      return key

    def value_to_dict(self, value, ctx):
        """Returns a dict representation of a value instance for serialization.
        Args:
            value: Entity instance.
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.
        Returns:
            dict: Dict populated with entity attributes to be serialized.
        """
        return value

    def write_result(self, record_value: dict) -> None:
        """Write analysis result to Kafka topic.

        Args:
          record_value (dict): The analysis result to send to Kafka.

        Return(s):
          None
        """
        try:
            key_dict = {
                "analysis_start_time_epoch": self.analysis_start_time,
                "kafka_cluster_id": self.kafka_cluster_id,
                "topic_name": self.topic_name
            }
            # Producer.produce() is thread-safe, no lock needed here
            self.producer.produce(topic=self.topic_name,
                                    key=self.json_key_serializer(key_dict, SerializationContext(self.topic_name, MessageField.KEY)),
                                    value=self.json_value_serializer(record_value, SerializationContext(self.topic_name, MessageField.VALUE)),
                                    on_delivery=self.delivery_callback)

            # Poll to handle callbacks (thread-safe)
            self.producer.poll(0)
        except BufferError:
            # Queue is full, wait and retry
            logging.warning("Producer queue full, waiting...")
            self.producer.poll(1)  # Wait up to 1 second
            self.producer.produce(topic=self.topic_name,
                                    key=self.json_key_serializer(key_dict, SerializationContext(self.topic_name, MessageField.KEY)),
                                    value=self.json_value_serializer(record_value, SerializationContext(self.topic_name, MessageField.VALUE)),
                                    on_delivery=self.delivery_callback)
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
