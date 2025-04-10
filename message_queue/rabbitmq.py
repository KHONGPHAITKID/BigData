import pika
import json
from message_queue.interface import MessageQueueBase, Message
from typing import Dict, Any, List
from datetime import datetime
import threading
import time
from collections import defaultdict
import random

class RabbitMQ(MessageQueueBase):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.channels = []
        self.publisher = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config['host'],
                port=self.config['port'],
                credentials=pika.PlainCredentials(
                    self.config['username'],
                    self.config['password']
                )
            )
        ).channel(channel_number=1000)
        self.queue = config['queue']
        self.num_queues = config['num_queues']
        self.messages = [[] for _ in range(self.num_queues + 1)]
        self.message_count = defaultdict(int)
        self.last_message_time = defaultdict(float)
        self.consumer_threads = []
        self.is_consuming = [False for _ in range(self.num_queues + 1)]
        self.stop_event = threading.Event()
        for i in range(self.num_queues):
            self.channels.append(self._get_channel(i))

    def _get_channel(self, channel_idx):
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config['host'],
                port=self.config['port'],
                credentials=pika.PlainCredentials(
                    self.config['username'],
                    self.config['password']
                )
            )
        )
        return conn.channel(channel_number=channel_idx+1)

    def connect(self) -> bool:
        for channel in self.channels:
            print(f"Declaring queue {self.queue}-{channel.channel_number}")
            channel.queue_declare(
                queue=f'{self.queue}-{channel.channel_number}'
            )
            channel.basic_consume(
                queue=f'{self.queue}-{channel.channel_number}',
                on_message_callback=self.callback,
                auto_ack=True
            )
            self.last_message_time[channel.channel_number] = time.time()
        return True

    def produce(self, messages: List[Message]):
        for idx, message in enumerate(messages):
            message_dict = message.to_dict()
            if isinstance(message_dict['produce_time'], datetime):
                message_dict['produce_time'] = (
                    message_dict['produce_time'].isoformat()
                )
            if isinstance(message_dict['consume_time'], datetime):
                message_dict['consume_time'] = (
                    message_dict['consume_time'].isoformat()
                )
            message_json = json.dumps(message_dict)
            self.publisher.basic_publish(
                exchange='',
                routing_key=f'{self.queue}-{idx % self.num_queues + 1}',
                body=message_json
            )

    def consume(self) -> List[Message]:
        for channel in self.channels:
            t = threading.Thread(
                target=self._consume_channel,
                args=(channel, channel.channel_number),
                name=f"Consumer-{channel.channel_number}"
            )
            t.start()
            self.consumer_threads.append(t)

        # Wait for all threads to complete
        for t in self.consumer_threads:
            t.join()

        # Print total message count
        total_messages = sum(self.message_count.values())
        print(f"Total messages received across all channels: {total_messages}")

        return [Message.from_dict(json.loads(message)) for message in self.messages]

    def _consume_channel(self, channel, channel_idx):
        t = threading.Thread(target=self._stop_consuming, args=(channel, channel_idx), name=f"Consumer-{channel_idx}")
        t.start()
        print(f"Consumer {channel_idx} started")
        channel.start_consuming()
        print(f"Consumer {channel_idx} finished")
        t.join()

    def _stop_consuming(self, channel, channel_idx):
        while True:
            current_time = time.time()
            if (current_time - self.last_message_time[channel_idx]) > 4:
                print(f"Channel {channel_idx} timed out after 5 seconds of inactivity")
                time.sleep(random.randint(1, 5))
                channel.stop_consuming()
                break

    def callback(self, ch, method, properties, body):
        channel_idx = ch.channel_number
        self.message_count[channel_idx] += 1
        self.last_message_time[channel_idx] = time.time()
        # print(f"Channel {channel_idx} received message: {body.decode('utf-8')}")
        self.messages[channel_idx].append(body)
        return body

    def close(self):
        self.stop_event.set()
        for t in self.consumer_threads:
            t.join(timeout=1)

    def is_connected(self) -> bool:
        return self.connection.is_open
