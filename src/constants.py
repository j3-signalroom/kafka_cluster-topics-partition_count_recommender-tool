from typing import Final


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Default configuration constants
DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_KAFKA_CREDENTIALS: Final[str] = "False"
DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR: Final[float] = 3.0
DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS: Final[int] = 50
DEFAULT_SAMPLING_DAYS: Final[int] = 7
DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES: Final[int] = 5
DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE: Final[int] = 1000
DEFAULT_SAMPLING_MAXIMUM_BATCH_SIZE: Final[int] = 25000
DEFAULT_SAMPLING_BATCH_SIZE: Final[int] = 10000
DEFAULT_SAMPLING_TIMEOUT_SECONDS: Final[float] = 2.0
DEFAULT_USE_SAMPLE_RECORDS: Final[str] = "True"
DEFAULT_USE_AWS_SECRETS_MANAGER: Final[str] = "False"
DEFAULT_INCLUDE_INTERNAL_TOPICS: Final[str] = "False"

# Character repeat limit for string fields
DEFAULT_CHARACTER_REPEAT: Final[int] = 100

# Consumer throughput threshold and minimum recommended partitions
DEFAULT_CONSUMER_THROUGHPUT_THRESHOLD: Final[int] = 1024 * 1024 * 10  # 10 MB/s
DEFAULT_MINIMUM_RECOMMENDED_PARTITIONS: Final[int] = 6

# Multithreading configuration
DEFAULT_MAX_CLUSTER_WORKERS: Final[int] = 4
DEFAULT_MAX_WORKERS_PER_CLUSTER: Final[int] = 8

# RESTful API configuration
DEFAULT_RESTFUL_API_MAX_RETRIES: Final[int] = 3

# Logging configuration
DEFAULT_TOOL_LOG_FILE: Final[str] = "kafka-cluster-topics-partition-count-recommender-tool.log"
DEFAULT_TOOL_LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Kafka writer configuration
DEFAULT_USE_KAFKA_WRITER: Final[str] = "False"
DEFAULT_KAFKA_WRITER_TOPIC_NAME: Final[str] = "_j3.partition_recommender.results"
DEFAULT_KAFKA_WRITER_TOPIC_PARTITION_COUNT: Final[int] = 6
DEFAULT_KAFKA_WRITER_TOPIC_REPLICATION_FACTOR: Final[int] = 3
DEFAULT_KAFKA_WRITER_TOPIC_DATA_RETENTION_IN_DAYS: Final[int] = 0  # 0 means infinite retention
