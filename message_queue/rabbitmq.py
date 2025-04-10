import pika
import json
import logging
from message_queue.interface import MessageQueueBase, Message
from typing import Dict, Any, List, Optional
from datetime import datetime
import threading
import time
from collections import defaultdict
import random

# Disable all logging
logging.getLogger("pika").setLevel(logging.CRITICAL)
logger = logging.getLogger("rabbitmq")
logger.addHandler(logging.NullHandler())
logger.propagate = False
logger.disabled = True

class RabbitMQ(MessageQueueBase):
    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.queue = config['queue']
        self.num_queues = config['num_queues']

        # Connection and channel tracking
        self.publisher_connection = None
        self.publisher = None
        self.channels = []
        self.channel_connections = []

        # Message tracking
        self.messages = [[] for _ in range(self.num_queues + 1)]
        self.message_count = defaultdict(int)
        self.last_message_time = defaultdict(float)

        # Thread management
        self.consumer_threads = []
        self.stop_event = threading.Event()

        # Connection status
        self.connected = False

        # Queue setup options
        self.purge_queues = config.get('purge_queues', False)
        self.delete_queues = config.get('delete_queues', False)
        self.durable = config.get('durable', True)
        self.auto_delete = config.get('auto_delete', False)

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Create connection parameters from config."""
        return pika.ConnectionParameters(
            host=self.config.get('host', 'localhost'),
            port=self.config.get('port', 5672),
            virtual_host=self.config.get('virtual_host', '/'),
            credentials=pika.PlainCredentials(
                self.config.get('username', 'guest'),
                self.config.get('password', 'guest')
            ),
            heartbeat=self.config.get('heartbeat', 60),
            connection_attempts=self.config.get('connection_attempts', 3),
            retry_delay=self.config.get('retry_delay', 5),
            socket_timeout=self.config.get('socket_timeout', 10)
        )

    def _initialize_publisher(self) -> bool:
        """Initialize the publisher connection and channel."""
        try:
            self.publisher_connection = pika.BlockingConnection(
                self._get_connection_params()
            )
            self.publisher = self.publisher_connection.channel(channel_number=1000)
            return True
        except Exception:
            return False

    def _initialize_channels(self) -> bool:
        """Initialize consumer channels."""
        success = True
        try:
            self.channels = []
            self.channel_connections = []

            for i in range(self.num_queues):
                try:
                    conn = pika.BlockingConnection(self._get_connection_params())
                    channel = conn.channel(channel_number=i+1)
                    self.channel_connections.append(conn)
                    self.channels.append(channel)
                except Exception:
                    success = False

            if self.channels:
                return True
            else:
                return False
        except Exception:
            return False

    def _setup_queue(self, channel, queue_name):
        """Setup a queue with proper error handling for existing queues."""
        try:
            # Check if queue exists and delete if configured to do so
            if self.delete_queues:
                try:
                    print(f"Deleting queue {queue_name} if it exists")
                    channel.queue_delete(queue=queue_name)
                except Exception as e:
                    # Queue might not exist, which is fine
                    print(f"Could not delete queue {queue_name}: {e}")

            # Declare the queue with specified properties
            print(f"Declaring queue {queue_name} with durable={self.durable}")
            channel.queue_declare(
                queue=queue_name,
                durable=self.durable,
                auto_delete=self.auto_delete
            )

            # Purge queue if configured
            if self.purge_queues:
                try:
                    print(f"Purging queue {queue_name}")
                    channel.queue_purge(queue=queue_name)
                except Exception as e:
                    print(f"Could not purge queue {queue_name}: {e}")

            return True
        except pika.exceptions.ChannelClosedByBroker as e:
            # Handle PRECONDITION_FAILED errors
            if e.reply_code == 406:
                print(f"Queue exists with different properties: {e}")
                # Try to reconnect the channel
                try:
                    # Close existing connection and create a new one
                    i = self.channels.index(channel)
                    if i >= 0:
                        self.channel_connections[i].close()
                        conn = pika.BlockingConnection(self._get_connection_params())
                        new_channel = conn.channel()
                        self.channel_connections[i] = conn
                        self.channels[i] = new_channel

                        # Force delete and recreate
                        new_channel.queue_delete(queue=queue_name)
                        new_channel.queue_declare(
                            queue=queue_name,
                            durable=self.durable,
                            auto_delete=self.auto_delete
                        )
                        return True
                except Exception as inner_e:
                    print(f"Failed to recover from queue mismatch: {inner_e}")
                    return False
            return False
        except Exception as e:
            print(f"Failed to set up queue {queue_name}: {e}")
            return False

    def connect(self) -> bool:
        """Connect to RabbitMQ and set up queues."""
        # Initialize publisher
        print("Initializing publisher")
        if not self._initialize_publisher():
            return False

        # Initialize channels
        print("Initializing channels")
        if not self._initialize_channels():
            return False

        # Set up queues
        print("Setting up queues")
        try:
            for i, channel in enumerate(self.channels):
                queue_name = f'{self.queue}-{i+1}'

                # Setup queue with proper error handling
                if not self._setup_queue(channel, queue_name):
                    print(f"Failed to set up queue {queue_name}")
                    continue

                # Set up consumer
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self.callback,
                    auto_ack=True
                )

                self.last_message_time[i+1] = time.time()

            if all(channel.is_open for channel in self.channels):
                self.connected = True
                print("Successfully connected to RabbitMQ")
                return True
            else:
                print("Some channels failed to open")
                self.connected = False
                return False
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.connected = False
            return False

    def produce(self, messages: List[Message]) -> bool:
        """Produce messages to RabbitMQ queues."""
        if not self.is_connected():
            return False

        try:
            for idx, message in enumerate(messages):
                message.produce_time = datetime.now()
                message_dict = message.to_dict()
                self._serialize_datetime_fields(message_dict)

                message_json = json.dumps(message_dict)
                queue_idx = idx % self.num_queues + 1
                routing_key = f'{self.queue}-{queue_idx}'

                properties = pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )

                self.publisher.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=message_json,
                    properties=properties
                )
            return True
        except pika.exceptions.AMQPConnectionError:
            # Try to reconnect
            if self._initialize_publisher():
                return self.produce(messages)
            return False
        except Exception:
            return False

    def _serialize_datetime_fields(self, message_dict: Dict[str, Any]) -> None:
        """Convert datetime objects to ISO format strings."""
        for field in ['produce_time', 'consume_time']:
            if field in message_dict and isinstance(message_dict[field], datetime):
                message_dict[field] = message_dict[field].isoformat()

    def consume(self) -> List[Message]:
        """Consume messages from all queues."""
        if not self.is_connected():
            print("Not connected")
            return []

        try:
            print("Consuming messages")
            # Clear previous state
            self.consumer_threads = []
            self.stop_event.clear()

            # Start consumer threads
            for i, channel in enumerate(self.channels):
                channel_idx = i + 1
                t = threading.Thread(
                    target=self._consume_channel,
                    args=(channel, channel_idx),
                    name=f"Consumer-{channel_idx}"
                )
                t.daemon = True  # Make threads daemon to avoid blocking on program exit
                t.start()
                self.consumer_threads.append(t)

            # Wait for all threads to complete
            for i, t in enumerate(self.consumer_threads):
                t.join(timeout=self.config.get('consume_timeout', 30))

            # Process messages
            return self._process_messages()
        except Exception:
            return []

    def _consume_channel(self, channel: pika.channel.Channel, channel_idx: int) -> None:
        """Consume messages from a specific channel."""
        # Start monitoring thread for this channel
        monitor_thread = threading.Thread(
            target=self._monitor_activity,
            args=(channel, channel_idx),
            name=f"Monitor-{channel_idx}"
        )
        monitor_thread.daemon = True
        monitor_thread.start()

        try:
            channel.start_consuming()
        except Exception:
            pass
        finally:
            try:
                monitor_thread.join(timeout=2)
            except Exception:
                pass

    def _monitor_activity(self, channel: pika.channel.Channel, channel_idx: int) -> None:
        """Monitor channel activity and stop consuming if idle."""
        try:
            timeout_seconds = self.config.get('idle_timeout', 10)

            while not self.stop_event.is_set():
                current_time = time.time()
                if (current_time - self.last_message_time[channel_idx]) > timeout_seconds:
                    time.sleep(0.5)  # Small delay before stopping
                    try:
                        if channel.is_open:
                            channel.stop_consuming()
                    except Exception:
                        pass
                    break
                time.sleep(0.5)
        except Exception:
            pass

    def callback(self, ch, method, properties, body) -> bytes:
        """Process incoming messages."""
        try:
            channel_idx = ch.channel_number
            self.message_count[channel_idx] += 1
            self.last_message_time[channel_idx] = time.time()

            # Add current timestamp to the message as consume_time
            try:
                # Parse the message and update the consume_time
                if isinstance(body, bytes):
                    message_str = body.decode('utf-8')
                else:
                    message_str = body

                message_dict = json.loads(message_str)
                message_dict['consume_time'] = datetime.now().isoformat()

                # Reserialize the message with the updated consume_time
                updated_body = json.dumps(message_dict).encode('utf-8')
                self.messages[channel_idx].append(updated_body)
            except Exception:
                # If we can't update the consume_time, store the original message
                self.messages[channel_idx].append(body)

            return body
        except Exception:
            return body

    def _process_messages(self) -> List[Message]:
        """Process received messages into Message objects."""
        result = []
        for channel_idx, channel_messages in enumerate(self.messages):
            for message in channel_messages:
                try:
                    # Ensure message is properly decoded
                    if isinstance(message, bytes):
                        message_str = message.decode('utf-8')
                    else:
                        message_str = message

                    # Parse JSON and create Message object
                    message_dict = json.loads(message_str)

                    # Convert ISO format string to datetime if needed
                    # Convert consume_time from ISO format string to datetime if needed
                    if isinstance(message_dict.get('consume_time'), str):
                        try:
                            message_dict['consume_time'] = datetime.fromisoformat(message_dict['consume_time'])
                        except ValueError:
                            # If parsing fails, use current time
                            print("Failed to parse consume_time")
                            message_dict['consume_time'] = datetime.now()

                    # Convert produce_time from ISO format string to datetime if needed
                    if isinstance(message_dict.get('produce_time'), str):
                        try:
                            message_dict['produce_time'] = datetime.fromisoformat(message_dict['produce_time'])
                        except ValueError:
                            # If parsing fails, use current time
                            print("Failed to parse produce_time")
                            message_dict['produce_time'] = None

                    result.append(Message.from_dict(message_dict))
                except (TypeError, json.JSONDecodeError):
                    pass

        # Clear messages after processing
        self.messages = [[] for _ in range(self.num_queues + 1)]
        return result

    def close(self) -> bool:
        """Close all connections to RabbitMQ."""
        self.stop_event.set()
        success = True

        try:
            # Stop all consumer threads gracefully
            for idx, t in enumerate(self.consumer_threads):
                t.join(timeout=2)

            # Close channel connections
            for i, channel in enumerate(self.channels):
                try:
                    if channel.is_open:
                        channel.close()
                except Exception:
                    success = False

            # Close the channel connections
            for i, conn in enumerate(self.channel_connections):
                try:
                    if conn.is_open:
                        conn.close()
                except Exception:
                    success = False

            # Close the publisher connection
            if self.publisher and self.publisher.is_open:
                try:
                    self.publisher.close()
                except Exception:
                    success = False

            if self.publisher_connection and self.publisher_connection.is_open:
                try:
                    self.publisher_connection.close()
                except Exception:
                    success = False

            self.connected = False
            return success
        except Exception:
            self.connected = False
            return False

    def is_connected(self) -> bool:
        """Check if the connection to RabbitMQ is open."""
        try:
            # Check publisher connection
            publisher_connected = (self.publisher and self.publisher.is_open and
                                  self.publisher_connection and self.publisher_connection.is_open)

            # Check if we have at least one working channel
            channels_connected = any(channel.is_open for channel in self.channels if channel)

            return publisher_connected and channels_connected and self.connected
        except Exception:
            return False
