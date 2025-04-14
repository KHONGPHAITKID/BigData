from message_queue.interface import MessageQueueBase
from typing import List, Dict, Any
from benchmark.scenario import Scenario, LowThroughputScenario, MediumThroughputScenario, HighThroughputScenario, ConsumerDisconnectScenario, ExtremeThroughputScenario
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
        list_of_scenarios = [
            LowThroughputScenario(self.utils),
            MediumThroughputScenario(self.utils),
            HighThroughputScenario(self.utils),
            ConsumerDisconnectScenario(self.utils),
            ExtremeThroughputScenario(self.utils),
        ]
        message_queue = self.utils.message_queue
        
        for scenario in list_of_scenarios:
            message_queue.connect()
            print(f"Running {scenario.scenario_name}")
            self.scenario = scenario
            total_message, _, e2e_latency = self.scenario.run()
            average_latency = e2e_latency / total_message
            print(f"{self.scenario.scenario_name} complete. Total message: {total_message}, latency: {average_latency}, e2e_latency: {e2e_latency}")
            with open(f"benchmark/final_logs/{message_queue.name}_{self.scenario.scenario_name}_latency_list.txt", "w") as f:
                f.write(f"Total message: {total_message}, latency: {average_latency}, e2e_latency: {e2e_latency}")

            print("================================================")
            message_queue.close()

        
