from abc import ABC, abstractmethod
from datetime import datetime
from benchmark.utils import BenchmarkUtils
from message_queue.interface import Message
from typing import List
import time
import random

class Scenario(ABC):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        self.benchmark_utils = benchmark_utils
        self.scenario_name = None

    @abstractmethod
    def run(self) -> List[Message]:
        pass

    def set_benchmark_utils(self, benchmark_utils: BenchmarkUtils):
        self.benchmark_utils = benchmark_utils

class LowThroughputScenario(Scenario):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Low_Throughput"

    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        for _ in range(50):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=10, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        print(f"Low throughput benchmark complete. Consumed {len(consumed_messages)} messages.")

        return consumed_messages
    
class MediumThroughputScenario(Scenario):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Medium_Throughput"

    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        # Higher message count and larger batch size for high throughput
        for _ in range(200):  # Increased from 50 to 200 iterations
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=50, produce_time=produce_time)  # Increased from 10 to 50 messages per batch
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)  # Small delay between batches to prevent overwhelming the system

        while self.benchmark_utils.message_queue.is_consuming():    
            time.sleep(0.1)
        
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        print(f"Medium throughput benchmark complete. Consumed {len(consumed_messages)} messages.")

        return consumed_messages
    
class HighThroughputScenario(Scenario):
    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "High_Throughput"

    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        for _ in range(1000):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=100, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        self.benchmark_utils.message_queue.get_stats().draw_histogram("latency")

        return consumed_messages    
    

class ConsumerDisconnectScenario(Scenario):
    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Consumer_Disconnect"

    def run(self) -> List[Message]:
        self.benchmark_utils.message_queue.consume()

        for index in range(1000):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=100, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)

            if index == 500:
                consumer_index = random.randint(0, self.benchmark_utils.message_queue.consumer_count - 1)
                self.benchmark_utils.message_queue.stop_consumer(consumer_index)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        
        self.benchmark_utils.message_queue.running = False
        consumed_messages = self.benchmark_utils.message_queue.get_consumed_messages()
        print(f"Consumer disconnect benchmark complete. Consumed {len(consumed_messages)} messages.")
        self.benchmark_utils.message_queue.get_stats().draw_histogram("latency")

        return consumed_messages 
        
            