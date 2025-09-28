import tomllib
from pathlib import Path
import logging
import logging.config

from constants import (DEFAULT_TOOL_LOG_FILE, DEFAULT_TOOL_LOG_FORMAT)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def setup_logging(log_file: str = DEFAULT_TOOL_LOG_FILE) -> logging.Logger:
    """Load logging configuration from pyproject.toml.  If not found, use default logging.
    
    Arg(s):
        log_file (str): The log file name to use if no configuration is found.
        
    Return(s):
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
                format=DEFAULT_TOOL_LOG_FORMAT,
                filemode="w",  # This will reset the log file
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
    else:
        # Default logging setup if no pyproject.toml
        logging.basicConfig(
            level=logging.INFO,
            format=DEFAULT_TOOL_LOG_FORMAT,
            filemode="w",  # This will reset the log file
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    return logging.getLogger()
