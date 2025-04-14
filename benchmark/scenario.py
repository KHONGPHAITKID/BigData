from abc import ABC, abstractmethod
from datetime import datetime
from benchmark.utils import BenchmarkUtils
from message_queue.interface import Message
from typing import List, Tuple
import time
import random

class Scenario(ABC):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        self.benchmark_utils = benchmark_utils
        self.scenario_name = None

    @abstractmethod
    def run(self) -> Tuple[int, float, float]:
        pass

    def set_benchmark_utils(self, benchmark_utils: BenchmarkUtils):
        self.benchmark_utils = benchmark_utils

class LowThroughputScenario(Scenario):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Low_Throughput"

    def run(self) -> Tuple[int, float, float]:
        self.benchmark_utils.message_queue.consume()
        print("Hello world")
        for _ in range(50):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=10, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        end_time = datetime.now()

        self.benchmark_utils.message_queue.running = False
        
        total_consumed_messages = self.benchmark_utils.message_queue.get_total_consumed_messages()
        average_latency = self.benchmark_utils.message_queue.get_average_latency()
        e2e_latency = self.benchmark_utils.message_queue.get_e2e_latency()
        if e2e_latency == 0:
            e2e_latency = (end_time - self.benchmark_utils.message_queue.get_min_first_message_time()).total_seconds()

        return total_consumed_messages, average_latency, e2e_latency
    
class MediumThroughputScenario(Scenario):

    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Medium_Throughput"

    def run(self) -> Tuple[int, float, float]:
        self.benchmark_utils.message_queue.consume()

        # Higher message count and larger batch size for high throughput
        for _ in range(200):  # Increased from 50 to 200 iterations
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=50, produce_time=produce_time)  # Increased from 10 to 50 messages per batch
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)  # Small delay between batches to prevent overwhelming the system

        while self.benchmark_utils.message_queue.is_consuming():    
            time.sleep(0.1)
        end_time = datetime.now()

        self.benchmark_utils.message_queue.running = False
        total_consumed_messages = self.benchmark_utils.message_queue.get_total_consumed_messages()
        average_latency = self.benchmark_utils.message_queue.get_average_latency()
        e2e_latency = self.benchmark_utils.message_queue.get_e2e_latency()
        if e2e_latency == 0:
            e2e_latency = (end_time - self.benchmark_utils.message_queue.get_min_first_message_time()).total_seconds()

        return total_consumed_messages, average_latency, e2e_latency
    
class HighThroughputScenario(Scenario):
    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "High_Throughput"

    def run(self) -> Tuple[int, float, float]:
        self.benchmark_utils.message_queue.consume()

        for _ in range(1000):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=100, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        end_time = datetime.now()

        self.benchmark_utils.message_queue.running = False
        total_consumed_messages = self.benchmark_utils.message_queue.get_total_consumed_messages()
        average_latency = self.benchmark_utils.message_queue.get_average_latency()
        e2e_latency = self.benchmark_utils.message_queue.get_e2e_latency()
        if e2e_latency == 0:
            e2e_latency = (end_time - self.benchmark_utils.message_queue.get_min_first_message_time()).total_seconds()

        return total_consumed_messages, average_latency, e2e_latency
    

class ConsumerDisconnectScenario(Scenario):
    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Consumer_Disconnect"

    def run(self) -> Tuple[int, float, float]:
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
        end_time = datetime.now()
        
        self.benchmark_utils.message_queue.running = False
      
        total_consumed_messages = self.benchmark_utils.message_queue.get_total_consumed_messages()
        average_latency = self.benchmark_utils.message_queue.get_average_latency()
        e2e_latency = self.benchmark_utils.message_queue.get_e2e_latency()
        if e2e_latency == 0:
            e2e_latency = (end_time - self.benchmark_utils.message_queue.get_min_first_message_time()).total_seconds()

        return total_consumed_messages, average_latency, e2e_latency
        
            

class ExtremeThroughputScenario(Scenario):
    def __init__(self, benchmark_utils: BenchmarkUtils = None):
        super().__init__(benchmark_utils)
        self.scenario_name = "Extreme_Throughput"
        
        
    def run(self) -> Tuple[int, float, float]:
        self.benchmark_utils.message_queue.consume()

        for _ in range(10000):
            produce_time = datetime.now()
            messages = self.benchmark_utils.create_payload(size=100, produce_time=produce_time)
            self.benchmark_utils.message_queue.produce(messages)
            time.sleep(0.01)

        while self.benchmark_utils.message_queue.is_consuming():
            time.sleep(0.1)
        end_time = datetime.now()
        self.benchmark_utils.message_queue.running = False

        total_consumed_messages = self.benchmark_utils.message_queue.get_total_consumed_messages()
        average_latency = self.benchmark_utils.message_queue.get_average_latency()
        e2e_latency = self.benchmark_utils.message_queue.get_e2e_latency()

        if e2e_latency == 0:
            e2e_latency = (end_time - self.benchmark_utils.message_queue.get_min_first_message_time()).total_seconds()

        return total_consumed_messages, average_latency, e2e_latency
            
