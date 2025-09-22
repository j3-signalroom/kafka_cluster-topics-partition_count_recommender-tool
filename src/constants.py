from typing import Final


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Default configuration constants
DEFAULT_REQUIRED_CONSUMPTION_THROUGHPUT_FACTOR: Final[float] = 3.0
DEFAULT_SAMPLING_MAX_CONSECUTIVE_NULLS: Final[int] = 50
DEFAULT_SAMPLING_DAYS: Final[int] = 7
DEFAULT_SAMPLING_MAX_CONTINUOUS_FAILED_BATCHES: Final[int] = 5
DEFAULT_SAMPLING_MINIMUM_BATCH_SIZE: Final[int] = 1000
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