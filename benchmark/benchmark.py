from message_queue.interface import MessageQueueBase, Message
from typing import List
import uuid
from datetime import datetime
from benchmark.stats import Stats
from threading import Thread
from queue import Queue

class Benchmark:
    def __init__(self):
        self.message_queue = None

    def set_message_queue(self, message_queue: MessageQueueBase):
        self.message_queue = message_queue

    def create_payload(self, size: int, produce_time: datetime = None) -> List[Message]:
        return [Message(id=str(uuid.uuid4()), content=str(uuid.uuid4()), produce_time=produce_time, consume_time=None) for _ in range(size)]

    def run(self):
        # self.message_queue.connect()
        self.message_queue.set_stats(Stats())

        # Use a Queue to collect messages from the consumer thread
        message_queue = Queue()

        # Define consume function to capture results
        def consume_messages():
            consumed_messages = self.message_queue.consume()
            message_queue.put(consumed_messages)

        # Start the consumer thread with the new function
        t = Thread(target=consume_messages)
        t.start()

        produced_messages = []
        for _ in range(10000):  # Number of iterations
            produce_time = datetime.now()
            messages = self.create_payload(size=10)
            # print(f"Producing {len(messages)} messages at {produce_time}")
            produced_messages.extend(messages)
            self.message_queue.produce(messages)

        print("Waiting for consumer to finish " + str(len(produced_messages)) + " messages")
        t.join()

        # Get the consumed messages from the queue
        consumed_messages = message_queue.get() if not message_queue.empty() else []
        print(f"Consumed {len(consumed_messages)} messages")

        self.message_queue.close()

        latency = []
        for message in consumed_messages:
            latency.append((message.consume_time - message.produce_time).total_seconds())

        print(f"Average latency: {sum(latency) / len(latency)}")
