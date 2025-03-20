from confluent_kafka import Producer, Consumer
import json
from datetime import datetime
import time
from typing import List, Dict, Any
from message_queue.interface import MessageQueueBase, Message

class KafkaMessageQueue(MessageQueueBase):
    """
    Kafka implementation of the MessageQueueBase interface using Confluent Kafka.
    """
    def __init__(self):
        """Initialize the Kafka message queue with no connections."""
        super().__init__()
        self.producer = None
        self.consumer = None
        self.topic = None

    def connect(self, config: Dict[str, Any]) -> bool:
        """
        Connect to the Kafka cluster using the provided configuration.

        Args:
            config: Dictionary with 'topic', 'producer', and 'consumer' keys.
                    - 'topic': str, the Kafka topic name.
                    - 'producer': Dict, producer configuration (e.g., {'bootstrap.servers': 'localhost:9092'}).
                    - 'consumer': Dict, consumer configuration (e.g., {'bootstrap.servers': 'localhost:9092', 'group.id': 'my_group'}).

        Returns:
            bool: True if connection is established, False otherwise.
        """
        try:
            self.topic      = config['topic']
            producer_config = config['producer']
            consumer_config = config['consumer']
            self.producer   = Producer(producer_config)
            self.consumer   = Consumer(consumer_config)
            self.consumer.subscribe([self.topic])
            self.connected = True
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            self.connected = False
            return False

    def produce(self, messages: List[Message]) -> bool:
        """
        Produce a list of messages to the Kafka topic.

        Args:
            messages: List of Message objects to send.

        Returns:
            bool: True if messages were sent successfully, False otherwise.
        """
        if not self.connected or not self.producer:
            return False
        try:
            for msg in messages:
                # Serialize datetime fields to ISO format strings or None
                msg_dict = {
                    "id": msg.id,
                    "content": msg.content,
                    "produce_time": msg.produce_time.isoformat() if msg.produce_time else None,
                    "consume_time": msg.consume_time.isoformat() if msg.consume_time else None
                }
                json_msg = json.dumps(msg_dict)
                self.producer.produce(self.topic, value=json_msg.encode('utf-8'))
            self.producer.flush()
            return True
        except Exception as e:
            print(f"Failed to produce messages: {e}")
            return False

    def consume(self) -> List[Message]:
        """
        Consume messages from the Kafka topic.

        Returns:
            List[Message]: List of consumed Message objects.
        """
        if not self.connected or not self.consumer:
            return []
        messages = []
        start_time = time.time()
        # Poll for up to 1 second to collect messages
        while time.time() - start_time < 1.0:
            raw_msg = self.consumer.poll(timeout=0.1)
            if raw_msg is None:
                continue
            if raw_msg.error():
                print(f"Consumer error: {raw_msg.error()}")
                continue
            try:
                json_str = raw_msg.value().decode('utf-8')
                data = json.loads(json_str)
                # Deserialize datetime fields from ISO format strings
                produce_time = datetime.fromisoformat(data["produce_time"]) if data.get("produce_time") else None
                consume_time = datetime.now()
                message = Message(
                    id=data["id"],
                    content=data["content"],
                    produce_time=produce_time,
                    consume_time=consume_time
                )
                messages.append(message)
                # Record latency if both timestamps are available
                if produce_time and consume_time and self.stats:
                    latency = (consume_time - produce_time).total_seconds()
                    self.stats.histogram("latency", latency)
            except Exception as e:
                print(f"Failed to process message: {e}")
        return messages

    def close(self) -> bool:
        """
        Close the connections to the Kafka producer and consumer.

        Returns:
            bool: True if closed successfully, False otherwise.
        """
        try:
            if self.producer:
                self.producer.flush()
            if self.consumer:
                self.consumer.close()
            self.connected = False
            return True
        except Exception as e:
            print(f"Failed to close connections: {e}")
            self.connected = False
            return False