import time
from typing import List, Tuple
import matplotlib.pyplot as plt
from datetime import datetime

class Stats:
    def __init__(self):
        self.stats = {}

    def histogram(self, key: str, value: int):
        if key not in self.stats:
            self.stats[key] = []
        self.stats[key].append((value, time.time()))
    
    def get(self, key: str) -> List[Tuple[int, float]]:
        return self.stats.get(key, [])
    
    def get_log_path(self, key: str) -> str:
        return "benchmark/logs/" + key
    
    def print_log(self, key: str, filename: str = "stats.log"):
        filename = self.get_log_path(key)
        with open(filename, "w") as f:
            for value, timestamp in self.get(key):
                f.write(f"{value} {timestamp}\n")

    def draw_histogram(self, key: str, filename: str = "histogram.png"):
        values, timestamps = zip(*self.get(key))
        # Convert timestamps to seconds relative to first timestamp
        base_ts = timestamps[0]
        relative_times = [(ts - base_ts) for ts in timestamps]
        dates = [datetime.fromtimestamp(ts) for ts in relative_times]
        
        plt.figure(figsize=(10, 6))
        plt.plot(dates, values, marker='o')
        plt.gcf().autofmt_xdate()
        plt.xlabel('Time (seconds from start)')
        plt.ylabel('Value') 
        plt.title(f'{key} over time')
        plt.grid(True)
        
        if len(dates) > 0:
            end_time = dates[-1]
            start_time = dates[0]
            plt.xlim(start_time, end_time)
            
        filename = self.get_log_path(key)
        plt.savefig(filename)
        plt.close()

    def reset(self):
        self.stats = {}


if __name__ == "__main__":
    stats = Stats()
    stats.histogram("latency", 100)
    time.sleep(1)
    stats.histogram("latency", 200)
    time.sleep(1)
    stats.histogram("latency", 300)
    stats.draw_histogram("latency")
    stats.print_log("latency")