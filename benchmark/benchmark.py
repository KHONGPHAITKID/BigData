from message_queue.interface import MessageQueueBase
from typing import List
from benchmark.scenario import Scenario, LowThroughputScenario, HighThroughputScenario
from benchmark.utils import BenchmarkUtils

class Benchmark:
    def __init__(self):
        self.utils = BenchmarkUtils()
        self.scenario = None

    def set_message_queue(self, message_queue: MessageQueueBase):
        self.utils.set_message_queue(message_queue)

    def set_scenario(self, scenario: Scenario):
        self.scenario = scenario

    def run(self):
        self.scenario = HighThroughputScenario(self.utils)
        messages = self.scenario.run()

        latency_list = []
        if messages:
            for message in messages:
                if message.produce_time and message.consume_time:
                    latency_list.append((message.consume_time - message.produce_time).total_seconds())
            if latency_list:
                avg_latency = sum(latency_list) / len(latency_list)
                print(f"Average latency: {avg_latency:.6f} seconds")

            # print the latency list
            with open("latency_list.txt", "w") as f:
                for latency in latency_list:
                    f.write(f"{latency}\n")

            
        else:
            print("No messages to calculate latency")

        
