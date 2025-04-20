import time
from typing import List, Tuple
import matplotlib.pyplot as plt
from datetime import datetime
import os

class Stats:
    """
    A class to manage statistics, log them to files, and generate plots.
    
    Attributes:
        stats (dict): A dictionary storing statistics with keys mapping to lists of (value, timestamp) tuples.
        log_dir (str): The directory where logs and plots are saved.
    """
    
    def __init__(self, log_dir="benchmark/logs"):
        """
        Initialize the Stats object.
        
        Args:
            log_dir (str, optional): Directory for logs and plots. Defaults to "benchmark/logs".
        """
        self.stats = {}
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)  # Create the directory if it doesn't exist

    def histogram(self, key: str, value: int):
        """
        Record a statistic value with a timestamp.
        
        Args:
            key (str): The identifier for the statistic (e.g., "latency").
            value (int): The value to record.
        """
        if key not in self.stats:
            self.stats[key] = []
        self.stats[key].append((value, time.time()))

    def get(self, key: str) -> List[Tuple[int, float]]:
        """
        Retrieve the recorded statistics for a given key.
        
        Args:
            key (str): The identifier for the statistic.
        
        Returns:
            List[Tuple[int, float]]: List of (value, timestamp) tuples, or empty list if key not found.
        """
        return self.stats.get(key, [])

    def get_log_path(self, key: str) -> str:
        """
        Get the full file path for the log file.
        
        Args:
            key (str): The identifier for the statistic.
        
        Returns:
            str: The full path (e.g., "benchmark/logs/latency.log").
        """
        return os.path.join(self.log_dir, f"{key}.log")

    def get_plot_path(self, key: str, extension: str = ".png") -> str:
        """
        Get the full file path for the plot file.
        
        Args:
            key (str): The identifier for the statistic.
            extension (str, optional): File extension for the plot. Defaults to ".png".
        
        Returns:
            str: The full path (e.g., "benchmark/logs/latency_histogram.png").
        """
        return os.path.join(self.log_dir, f"{key}_histogram{extension}")

    def print_log(self, key: str, filename: str = None):
        """
        Write the statistics to a log file.
        
        Args:
            key (str): The identifier for the statistic.
            filename (str, optional): Custom filename for the log. If None, uses default from get_log_path.
        """
        if filename is None:
            filename = self.get_log_path(key)
        else:
            filename = os.path.join(self.log_dir, filename)
        with open(filename, "w") as f:
            for value, timestamp in self.get(key):
                f.write(f"{value} {timestamp}\n")

    def draw_histogram(self, key: str, filename: str = None):
        return
        """
        Generate and save a line plot of the statistics over time.
        
        Args:
            key (str): The identifier for the statistic.
            filename (str, optional): Custom filename for the plot. If None, uses default from get_plot_path.
        """
        if filename is None:
            filename = self.get_plot_path(key)
        else:
            filename = os.path.join(self.log_dir, filename)
        
        # Extract values and timestamps
        values, timestamps = zip(*self.get(key))
        base_ts = timestamps[0]
        relative_times = [(ts - base_ts) for ts in timestamps]
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        plt.plot(relative_times, values, marker='o')
        plt.xlabel('Time (seconds from start)')
        plt.ylabel('Value')
        plt.title(f'{key} over time')
        plt.grid(True)
        if len(relative_times) > 0:
            plt.xlim(0, relative_times[-1])
        plt.savefig(filename)
        plt.close()  # Close the figure to free memory

    def reset(self):
        """
        Clear all recorded statistics.
        """
        self.stats = {}