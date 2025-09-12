import tomllib
from pathlib import Path
import logging
import logging.config


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def setup_logging(log_file: str = "kafka-cluster-topics-partition-count-recommender-app.log") -> logging.Logger:
    """Load logging configuration from pyproject.toml.  If not found, use default logging.
    
    Args:
        log_file (str): The log file name to use if no configuration is found.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    pyproject_path = Path("pyproject.toml")
    
    if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
        
        # Extract logging config
        logging_config = config.get("tool", {}).get("logging", {})
        
        if logging_config:
            logging.config.dictConfig(logging_config)
        else:
            # Fallback to basic file logging
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
    else:
        # Default logging setup if no pyproject.toml
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    return logging.getLogger()
