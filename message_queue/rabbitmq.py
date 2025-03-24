import pika
import json
from message_queue.interface import MessageQueueBase, Message
from typing import Dict, Any
from datetime import datetime
import time


class RabbitMQ(MessageQueueBase):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consuming = False

    def connect(self) -> bool:
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.config['host'], port=self.config['port'], credentials=pika.PlainCredentials(self.config['username'], self.config['password'])))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.config['queue'])
        self.channel.basic_consume(queue=self.config['queue'], on_message_callback=self.callback, auto_ack=True)
        return True

    def produce(self, message: Message):
        message_dict = message.to_dict()
        # Convert datetime objects to ISO format strings
        if isinstance(message_dict['produce_time'], datetime):
            message_dict['produce_time'] = message_dict['produce_time'].isoformat()
        if isinstance(message_dict['consume_time'], datetime):
            message_dict['consume_time'] = message_dict['consume_time'].isoformat()
        message_json = json.dumps(message_dict)
        self.channel.basic_publish(exchange='', routing_key=self.config['queue'], body=message_json)
        print(f"Sent message: {message_json}")
        return True

    def consume(self):
        self.channel.start_consuming()
        time.sleep(1)
        self.channel.stop_consuming()

    def callback(self, ch, method, properties, body):
        print(f"Received message: {body.decode('utf-8')}")
        return body

    def close(self):
        self.connection.close()

    def is_connected(self) -> bool:
        return self.connection.is_open
