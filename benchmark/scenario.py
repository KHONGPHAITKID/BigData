from abc import ABC, abstractmethod
from datetime import datetime
from benchmark.utils import BenchmarkUtils
from message_queue.interface import Message
from typing import List
import time

class Scenario(ABC):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        self.benchmark_utils = benchmark_utils

    @abstractmethod
    def run(self) -> List[Message]:
        pass

    def set_benchmark_utils(self, benchmark_utils: BenchmarkUtils):
        self.benchmark_utils = benchmark_utils

class LowThroughputScenario(Scenario):
    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        for _ in range(50):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=10, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)

        time.sleep(20)
        
        self.benchmark_utils.message_queue.running = False
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        print(f"Benchmark complete. Consumed {len(consumed_messages)} messages.")
        self.benchmark_utils.message_queue.close()
        self.benchmark_utils.message_queue.get_stats().draw_histogram("latency")

        return consumed_messages
    
class HighThroughputScenario(Scenario):
    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        # Higher message count and larger batch size for high throughput
        for _ in range(200):  # Increased from 50 to 200 iterations
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=50, produce_time=produce_time)  # Increased from 10 to 50 messages per batch
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)  # Small delay between batches to prevent overwhelming the system

        time.sleep(30)  # Increased wait time to allow more messages to be processed
        
        self.benchmark_utils.message_queue.running = False
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        print(f"High throughput benchmark complete. Consumed {len(consumed_messages)} messages.")
        self.benchmark_utils.message_queue.close()
        self.benchmark_utils.message_queue.get_stats().draw_histogram("latency")

        return consumed_messages