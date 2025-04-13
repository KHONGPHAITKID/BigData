from message_queue.interface import MessageQueueBase, Message
from typing import List
from datetime import datetime
import uuid

class BenchmarkUtils:
    def __init__(self, message_queue: MessageQueueBase = None):
        self.message_queue = message_queue
    
    def set_message_queue(self, message_queue: MessageQueueBase):
        self.message_queue = message_queue

    def create_payload(self, size: int, produce_time: datetime) -> List[Message]:
        return [Message(id=str(uuid.uuid4()), content=str(uuid.uuid4()), produce_time=produce_time, consume_time=None) for _ in range(size)]
        
    
        