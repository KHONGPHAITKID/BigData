from message_queue.interface import MessageQueueInterface, Message
from typing import List
import uuid
from datetime import datetime
from benchmark.stats import Stats

class Benchmark:
    def __init__(self):
        self.message_queue = None

    def set_message_queue(self, message_queue: MessageQueueInterface):
        self.message_queue = message_queue

    def create_payload(self, size: int) -> List[Message]:
        return [Message(id=str(uuid.uuid4()), content=str(uuid.uuid4())) for _ in range(size)]

    def run(self):
        self.message_queue.connect()
        self.message_queue.set_stats(Stats())

        # Benchmark here

