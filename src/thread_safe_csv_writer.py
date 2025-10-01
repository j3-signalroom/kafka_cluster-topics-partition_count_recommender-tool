import csv
from typing import List
import threading


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Thread-safe CSV writer
class ThreadSafeCsvWriter:
    """Thread-safe CSV writer for concurrent operations."""
    
    def __init__(self, filename: str, headers: List[str]):
        """Initialize the CSV writer with a filename and headers.
        
        Args:
            filename (str): The name of the CSV file to write to.
            headers (List[str]): The list of header strings for the CSV file.
        """
        self.filename = filename
        self.lock = threading.Lock()
        
        # Create file with headers
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
    
    def write_row(self, row: List) -> None:
        """Write a row to the CSV file in a thread-safe manner.

        Arg(s):
            row (List): The list of values representing a row in the CSV file.

        Return(s):
            None
        """
        with self.lock:
            with open(self.filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(row)