from message_queue.interface import MessageQueueBase
from typing import List, Dict, Any
from benchmark.scenario import Scenario, LowThroughputScenario, MediumThroughputScenario, HighThroughputScenario, ConsumerDisconnectScenario
from benchmark.utils import BenchmarkUtils

class Benchmark:
    def __init__(self):
        self.utils = BenchmarkUtils()
        self.scenario = None

    def set_message_queue(self, message_queue: MessageQueueBase):
        self.utils.set_message_queue(message_queue)

    def set_scenario(self, scenario: Scenario):
        self.scenario = scenario

    def run(self, config: Dict[str, Any]):
        list_of_scenarios = [
            LowThroughputScenario(self.utils),
            MediumThroughputScenario(self.utils),
            HighThroughputScenario(self.utils),
            ConsumerDisconnectScenario(self.utils),
        ]
        message_queue = self.utils.message_queue
        
        for scenario in list_of_scenarios:
            message_queue.connect(config["kafka"])
            print(f"Running {scenario.scenario_name}")
            self.scenario = scenario
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
                with open(f"benchmark/logs/{self.scenario.scenario_name}_latency_list.txt", "w") as f:
                    for latency in latency_list:
                        f.write(f"{latency}\n")
                    
                    f.write(f"Average latency: {avg_latency:.6f} seconds\n")

                    # Calculate P90
                    p90_latency = sorted(latency_list)[int(0.9 * len(latency_list))]
                    print(f"P90 latency: {p90_latency:.6f} seconds")
                    f.write(f"P90 latency: {p90_latency:.6f} seconds")
                
            else:
                print("No messages to calculate latency")

            print("================================================")
            message_queue.close()

        
