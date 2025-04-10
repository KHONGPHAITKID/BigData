from message_queue.interface import MessageQueueBase
from typing import Dict, Any
from message_queue.kafka import KafkaMessageQueue, KAFKA
from message_queue.rabbitmq import RabbitMQ, RABBIT
from benchmark.stats import Stats


class MessageQueueFactory:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.message_queue = None

    def create_message_queue(self, message_queue_type: str) -> MessageQueueBase:
        stats = Stats()
        message_queue = None
        if message_queue_type == KAFKA:
            message_queue = KafkaMessageQueue(self.config[KAFKA])
        elif message_queue_type == RABBIT:
            message_queue = RabbitMQ(self.config[RABBIT])
        else:
            raise ValueError(f"Invalid message queue type: {message_queue_type}")
        message_queue.set_stats(stats)
        return message_queue
