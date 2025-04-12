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

RABBIT = "rabbitmq"

# Disable all logging
logging.getLogger("pika").setLevel(logging.CRITICAL)
logger = logging.getLogger("rabbitmq")
logger.addHandler(logging.NullHandler())
logger.propagate = False
logger.disabled = True

class RabbitMQ(MessageQueueBase):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(RABBIT)
        self.config = config
        self.queue = config['queue']
        self.num_queues = config['num_queues']

        self.latencies = {}

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
            heartbeat=self.config.get('heartbeat', 30),  # Lower heartbeat interval for faster detection
            connection_attempts=self.config.get('connection_attempts', 5),  # More connection attempts
            retry_delay=self.config.get('retry_delay', 2),  # Faster retry
            socket_timeout=self.config.get('socket_timeout', 15),  # Longer socket timeout
            blocked_connection_timeout=self.config.get('blocked_connection_timeout', 10),  # Add blocked connection timeout
            client_properties={'connection_name': f'rabbitmq-client-{random.randint(1000, 9999)}'}  # Add client properties for better tracking
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
            # print(f"Declaring queue {queue_name} with durable={self.durable}")
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
        # First close any existing connections
        if self.connected:
            print("Already connected, closing existing connections first")
            self.close()
            time.sleep(1)  # Small delay to ensure connections are fully closed
        
        # Initialize publisher
        # print("Initializing publisher")
        if not self._initialize_publisher():
            print("Failed to initialize publisher")
            return False

        # Initialize channels
        # print("Initializing channels")
        if not self._initialize_channels():
            print("Failed to initialize channels")
            # Clean up publisher connection
            try:
                if self.publisher and self.publisher.is_open:
                    self.publisher.close()
                if self.publisher_connection and self.publisher_connection.is_open:
                    self.publisher_connection.close()
            except Exception as e:
                print(f"Error closing publisher during failed connect: {e}")
            return False

        # Set up queues
        print("Setting up queues")
        try:
            successful_queues = 0
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
                successful_queues += 1

            if successful_queues > 0 and all(channel.is_open for channel in self.channels if channel):
                self.connected = True
                # print(f"Successfully connected to RabbitMQ with {successful_queues} queues")
                return True
            else:
                # print(f"Some or all channels failed to open. Successful queues: {successful_queues}")
                self.connected = False
                self.close()  # Clean up any partially open connections
                return False
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.connected = False
            self.close()  # Clean up any partially open connections
            return False

    def produce(self, messages: List[Message]):
        """Produce messages to RabbitMQ queues."""
        if not self.is_connected():
            print("Cannot produce messages: not connected to RabbitMQ")
            return False

        try:
            message_count = len(messages)
            # print(f"Producing {message_count} messages")
            
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

                # print(f"Publishing message {idx+1}/{message_count} to {routing_key}")
                self.publisher.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=message_json,
                    properties=properties
                )
            
            # print(f"Successfully produced {message_count} messages")
            return True
        except pika.exceptions.AMQPConnectionError as e:
            # print(f"AMQP Connection Error during produce: {e}")
            # Try to reconnect
            if self._initialize_publisher():
                return self.produce(messages)
            return False
        except Exception as e:
            print(f"Error during produce: {e}")
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
            self.running_consumers = len(self.channels)

            # Start consumer threads
            for i, channel in enumerate(self.channels):
                channel_idx = i + 1
                self.last_message_time[channel_idx] = None
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
            # return self._process_messages()
        except Exception:
            # return []
            pass
        
    def get_consumed_messages(self) -> List[Message]:
        """Get the consumed messages."""
        return self._process_messages()

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
            # BlockingConnection doesn't support these callbacks
            # Just start consuming directly
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"AMQP Connection Error in consumer {channel_idx}: {e}")
            # Mark the last message time to trigger timeout
            self.last_message_time[channel_idx] = 0
        except pika.exceptions.ConnectionClosedByBroker as e:
            print(f"Connection closed by broker in consumer {channel_idx}: {e}")
            self.last_message_time[channel_idx] = 0
        except Exception as e:
            print(f"Unexpected error in consumer {channel_idx}: {e}")
            self.last_message_time[channel_idx] = 0
        finally:
            try:
                monitor_thread.join(timeout=2)
            except Exception:
                pass

    def _monitor_activity(self, channel: pika.channel.Channel, channel_idx: int) -> None:
        """Monitor channel activity and stop consuming if idle."""
        try:
            timeout_seconds = self.config.get('idle_timeout', 10)
            check_interval = 0.5  # Check interval in seconds

            while not self.stop_event.is_set():
                if self.last_message_time[channel_idx] is None:
                    # print(f"Channel {channel_idx} has no messages")
                    time.sleep(check_interval)
                    continue
                
                current_time = time.time()
                time_since_last_message = current_time - self.last_message_time[channel_idx]
                
                # Only log every few seconds to reduce spam
                # if int(time_since_last_message) % 5 == 0:
                #     print(f"Channel {channel_idx}: {time_since_last_message:.1f}s since last message (timeout: {timeout_seconds}s)")
                
                # Check if connection is still active
                conn_index = channel_idx - 1
                connection_alive = (
                    conn_index < len(self.channel_connections) and 
                    self.channel_connections[conn_index].is_open
                )
                
                # Stop if idle timeout reached or connection lost
                if time_since_last_message > timeout_seconds or not connection_alive:
                    if not connection_alive:
                        print(f"Connection for channel {channel_idx} is no longer open")
                    else:
                        print(f"Channel {channel_idx} idle timeout ({timeout_seconds}s) reached")
                    
                    time.sleep(check_interval)  # Small delay before stopping
                    
                    try:
                        if connection_alive and channel.is_open:
                            try:
                                channel.stop_consuming()
                                print(f"Consumer {channel_idx} stopped consuming")
                            # except IndexError:
                            #     print(f"Stop consuming failed: connection already closed for consumer {channel_idx}")
                            except Exception as e:
                                pass
                                # print(f"Error stopping consumer {channel_idx}: {e}")
                        else:
                            print(f"Not attempting to stop consumption: connection already closed for consumer {channel_idx}")
                    except Exception as e:
                        print(f"Error checking connection status for consumer {channel_idx}: {e}")
                    
                    # Always decrement counter when exiting
                    self.running_consumers -= 1
                    break
                
                time.sleep(check_interval)
            
            print(f"Channel {channel_idx} monitoring thread exited")
        except Exception as e:
            print(f"Error in monitoring thread for channel {channel_idx}: {e}")
            # Ensure counter is decremented
            self.running_consumers -= 1

    def callback(self, ch, method, properties, body) -> bytes:
        """Process incoming messages."""
        try:
            channel_idx = ch.channel_number
            self.message_count[channel_idx] += 1
            message_count = self.message_count[channel_idx]
            self.last_message_time[channel_idx] = time.time()
            # print(f"Received message #{message_count} from channel {channel_idx}")

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
                # updated_body = json.dumps(message_dict).encode('utf-8')
                # self.messages[channel_idx].append(updated_body)
                self.latencies[channel_idx] = message_dict['consume_time'] - message_dict['produce_time']
                # Debug message contents (first 50 chars)
                # first_50 = message_str[:50] + "..." if len(message_str) > 50 else message_str
                # print(f"Message content: {first_50}")
            except Exception as e:
                print(f"Error processing message: {e}")
                # If we can't update the consume_time, store the original message
                # self.messages[channel_idx].append(body)

            return body
        except Exception as e:
            print(f"Callback error: {e}")
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
        # print("Closing RabbitMQ connections")
        self.stop_event.set()
        success = True

        try:
            # Stop all consumer threads gracefully
            # print(f"Stopping {len(self.consumer_threads)} consumer threads")
            for idx, t in enumerate(self.consumer_threads):
                try:
                    t.join(timeout=2)
                    print(f"Consumer thread {idx+1} stopped")
                except Exception as e:
                    print(f"Error stopping consumer thread {idx+1}: {e}")
                    success = False

            # Close channel connections
            # print(f"Closing {len(self.channels)} channels")
            for i, channel in enumerate(self.channels):
                try:
                    if channel and hasattr(channel, 'is_open') and channel.is_open:
                        channel.close()
                        # print(f"Channel {i+1} closed")
                except Exception as e:
                    print(f"Error closing channel {i+1}: {e}")
                    success = False

            # Close the channel connections
            # print(f"Closing {len(self.channel_connections)} channel connections")
            for i, conn in enumerate(self.channel_connections):
                try:
                    if conn and hasattr(conn, 'is_open') and conn.is_open:
                        conn.close()
                        # print(f"Connection {i+1} closed")
                except Exception as e:
                    print(f"Error closing connection {i+1}: {e}")
                    success = False

            # Close the publisher connection
            if self.publisher:
                try:
                    if hasattr(self.publisher, 'is_open') and self.publisher.is_open:
                        self.publisher.close()
                        # print("Publisher channel closed")
                except Exception as e:
                    # print(f"Error closing publisher channel: {e}")
                    success = False

            if self.publisher_connection:
                try:
                    if hasattr(self.publisher_connection, 'is_open') and self.publisher_connection.is_open:
                        self.publisher_connection.close()
                        # print("Publisher connection closed")
                except Exception as e:
                    # print(f"Error closing publisher connection: {e}")
                    success = False

            # Reset connection state
            self.connected = False
            self.channels = []
            self.channel_connections = []
            self.publisher = None
            self.publisher_connection = None
            self.consumer_threads = []
            
            # print(f"RabbitMQ connections closed, success={success}")
            return success
        except Exception as e:
            print(f"Error during close: {e}")
            self.connected = False
            return False
        
    def get_num_consumed_messages(self) -> int:
        """
        Get the number of consumed messages.
        """
        return sum(self.message_count.values())

    def is_connected(self) -> bool:
        """Check if the connection to RabbitMQ is open."""
        try:
            # Check publisher connection
            publisher_ok = False
            channels_ok = False
            
            try:
                publisher_ok = (self.publisher and self.publisher.is_open and
                              self.publisher_connection and self.publisher_connection.is_open)
                
                # Test the connection by performing a lightweight operation
                if publisher_ok:
                    # Try to get the channel's state - this will raise an exception if connection is broken
                    _ = self.publisher.is_open
                    # print("Publisher connection verified")
            except Exception as e:
                print(f"Publisher connection check failed: {e}")
                publisher_ok = False
                
            # Check if we have at least one working channel
            try:
                open_channels = 0
                for channel in self.channels:
                    if channel and channel.is_open:
                        # Try to verify channel is really open
                        _ = channel.is_open  # Access property to trigger exception if broken
                        open_channels += 1
                
                channels_ok = open_channels > 0
                if channels_ok:
                    # print(f"{open_channels} open consumer channels detected")
                    pass
            except Exception as e:
                print(f"Channel connection check failed: {e}")
                channels_ok = False

            is_connected = publisher_ok and channels_ok and self.connected
            if not is_connected:
                print(f"Connection status check: publisher={publisher_ok}, channels={channels_ok}, self.connected={self.connected}")
            
            return is_connected
        except Exception as e:
            print(f"Error checking connection status: {e}")
            return False
