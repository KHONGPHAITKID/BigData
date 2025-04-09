from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import json
from datetime import datetime
import time
from typing import List, Dict, Any
from message_queue.interface import MessageQueueBase, Message
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import threading  # Changed from 'from threading import Thread' to import the whole module
from threading import Lock

class KafkaMessageQueue(MessageQueueBase):
    """
    Kafka implementation of the MessageQueueBase interface using Confluent Kafka.
    """
    def __init__(self):
        """Initialize the Kafka message queue with no connections."""
        super().__init__()
        self.producer = None
        self.consumers = []
        self.topic = None
        self.consumer_count = 5
        self.messages = []  # Buffer for consumed messages
        self.consumer_threads = []
        self.consuming = False
        self.lock = Lock()
        self.consume_timeout = 10  # Default timeout in seconds

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
            self.topic = config['topic']
            producer_config = config['producer']
            consumer_config = config['consumer']
            
            if 'group.id' not in consumer_config:
                raise ValueError("Consumer configuration must include 'group.id'")
            
            self.producer = Producer(producer_config)
            self.consumer_count = config['partition']
            self.consumers = [Consumer(consumer_config) for _ in range(self.consumer_count)]
            for i in range(self.consumer_count):
                self.consumers[i].subscribe([self.topic])
            
            self.connected = True
            logging.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            self.connected = False
            return False

    def _consumer_loop(self, consumer: Consumer):
        """Individual consumer processing loop"""
        logging.info(f"Starting consumer loop for {threading.current_thread().name}")  # Fixed
        print(f"Starting consumer loop for {threading.current_thread().name}")  # Fixed
        
        while self.consuming:
            try:
                msg = consumer.poll(timeout=2.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logging.error(f"Consumer error: {msg.error()}")
                    if msg.error().fatal():
                        break
                    continue
                
                json_str = msg.value().decode('utf-8')
                data = json.loads(json_str)
                
                produce_time = datetime.fromisoformat(data["produce_time"]) if data.get("produce_time") else None
                consume_time = datetime.now()
                message = Message(
                    id=data["id"],
                    content=data["content"],
                    produce_time=produce_time,
                    consume_time=consume_time
                )
                with self.lock:
                    self.messages.append(message)  # Store messages for return
                
                if produce_time and consume_time and self.stats:
                    latency = (consume_time - produce_time).total_seconds()
                    self.stats.histogram("latency", latency)
            except KafkaException as ke:
                logging.error(f"Kafka-specific error: {ke}")
                if "closed" in str(ke).lower():
                    break
                time.sleep(1)
            except json.JSONDecodeError as jde:
                logging.error(f"Failed to decode message JSON: {jde}")
            except Exception as e:
                logging.error(f"Unexpected error in consumer loop: {e}")
                if "closed" in str(e).lower():
                    break
                time.sleep(1)
        logging.info(f"Consumer loop for {threading.current_thread().name} terminated")
        print(f"Consumer loop for {threading.current_thread().name} terminated")

    def produce(self, messages: List[Message]) -> bool:
        """
        Produce a list of messages to the Kafka topic.

        Args:
            messages: List of Message objects to send.

        Returns:
            bool: True if messages were sent successfully, False otherwise.
        """
        if not self.connected or not self.producer:
            raise Exception("Producer is not connected")
        try:
            for msg in messages:
                # Serialize datetime fields to ISO format strings or None
                print("Produce message " + msg.id)
                msg_dict = {
                    "id": msg.id,
                    "content": msg.content,
                    "produce_time": datetime.now().isoformat(),
                    "consume_time": msg.consume_time.isoformat() if msg.consume_time else None
                }
                json_msg = json.dumps(msg_dict)
                self.producer.produce(self.topic, value=json_msg.encode('utf-8'))
            self.producer.flush()
            return True
        except Exception as e:
            logging.error(f"Failed to produce messages: {e}")
            return False

    def consume(self):
        """
        Start consuming messages from the Kafka topic in background threads.
        This is non-blocking.
        """
        if not self.connected:
            raise RuntimeError("Not connected to Kafka")
            
        logging.info("Starting consumption")
        print("Starting consumption")
        
        self.consuming = True
        self.consumer_threads = []
        for idx, consumer in enumerate(self.consumers):
            logging.info(f"Creating thread for consumer {idx}")
            print(f"Creating thread for consumer {idx}")
            t = threading.Thread(
                target=self._consumer_loop,
                args=(consumer,),
                daemon=True,  # Make threads daemon so they don't block program exit
                name=f"Consumer-{idx}"
            )
            t.start()
            self.consumer_threads.append(t)
    
    def get_consumed_messages(self) -> List[Message]:
        """
        Get all messages that have been consumed so far.
        This doesn't stop the consumption threads.
        
        Returns:
            List[Message]: List of consumed Message objects.
        """
        with self.lock:
            messages = self.messages.copy()
            self.messages.clear()
        logging.info(f"Retrieved {len(messages)} consumed messages")
        print(f"Retrieved {len(messages)} consumed messages")
        return messages
    
    def close(self) -> bool:
        """
        Close the connections to the Kafka producer and consumer.

        Returns:
            bool: True if closed successfully, False otherwise.
        """
        logging.info("Closing Kafka connections")
        print("Closing Kafka connections")
        self.consuming = False
        try:
            for thread in self.consumer_threads:
                thread.join(timeout=2.0)
                if thread.is_alive():
                    logging.warning(f"Thread {thread.name} did not terminate in time")
            
            if self.producer:
                self.producer.flush()
                self.producer = None
            
            if self.consumers:
                for consumer in self.consumers:
                    try:
                        consumer.close()
                    except Exception as e:
                        logging.error(f"Error closing consumer: {e}")
                self.consumers.clear()
            
            self.connected = False
            logging.info("Kafka connections closed successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to close connections: {e}")
            self.connected = False
            return False
        
class KafkaTopicManager:
    """
    Manages Kafka topics including creation and partition checks.
    """
    def __init__(self, bootstrap_servers='localhost:9093'):
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def create_topic(self, topic_name: str, num_partitions=5, replication_factor=1) -> bool:
        """
        Create a topic with specified partitions (default=5).
        """
        # Check if topic exists
        existing_topics = self.admin_client.list_topics().topics
        if topic_name in existing_topics:
            print(f"Topic {topic_name} already exists.")
            return False
        
        # Create new topic
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        # Execute topic creation
        futures = self.admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()  # Wait for operation
                print(f"Created topic {topic} with {num_partitions} partitions")
                return True
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
                return False

    def get_partition_count(self, topic_name: str) -> int:
        """
        Get number of partitions for a topic.
        """
        try:
            metadata = self.admin_client.list_topics(topic_name)
            return len(metadata.topics[topic_name].partitions)
        except Exception as e:
            print(f"Error getting partitions: {e}")
            return 0

    def topic_exists(self, topic_name: str) -> bool:
        """
        Check if topic exists in the cluster.
        """
        return topic_name in self.admin_client.list_topics().topics
