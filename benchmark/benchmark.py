from message_queue.interface import MessageQueueBase, Message
from typing import List
import uuid
from datetime import datetime
from benchmark.stats import Stats

class Benchmark:
    def __init__(self):
        self.message_queue = None

    def set_message_queue(self, message_queue: MessageQueueBase):
        self.message_queue = message_queue

    def create_payload(self, size: int, produce_time: datetime) -> List[Message]:
        return [Message(id=str(uuid.uuid4()), content=str(uuid.uuid4()), produce_time=produce_time, consume_time=None) for _ in range(size)]

    def run(self):
        # self.message_queue.connect()
        self.message_queue.set_stats(Stats())

        for _ in range(50):  # Number of iterations
            produce_time = datetime.now()
            messages = self.create_payload(size=10, produce_time=produce_time)
            self.message_queue.produce(messages)
            self.message_queue.consume()  # Stats recorded here
        self.message_queue.close()