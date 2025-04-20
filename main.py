# docker-compose up -d
# docker-compose ps
# docker-compose exec python-app bash
# python main.py

from benchmark.benchmark import Benchmark
from message_queue.factory import MessageQueueFactory
from message_queue.rabbitmq import RABBIT
from message_queue.kafka import KAFKA
import argparse
import os

# Get broker address from environment variable, with fallback logic
# Use KAFKA_BROKER env var if set, otherwise determine based on environment
kakfa_broker = os.environ.get('KAFKA_BROKER')
if not kakfa_broker:
    # Logic to detect if running inside Docker
    is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
    kafka_broker = "kafka:9092" if is_docker else "localhost:9093"

# Configuration for Kafka (adjust as needed)
config = {
    "kafka": {
        "topic": "live_demo",
        "partition": 5,
        "replication_factor": 1,
        "producer": {"bootstrap.servers": kafka_broker},
        "consumer": {
            "bootstrap.servers": kafka_broker,
            "group.id": "test_group",
            "auto.offset.reset": "earliest"
        }
    },
    "rabbitmq": {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'queue': 'live_demo',
        'num_queues': 5,
    }
}

if __name__ == "__main__":
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='Run message queue benchmark')
    parser.add_argument('--queue', type=str, choices=[KAFKA, RABBIT], default=KAFKA,
                        help='Message queue type to use (kafka or rabbitmq)')
    args = parser.parse_args()
    
    message_queue_factory = MessageQueueFactory(config)
    message_queue = message_queue_factory.create_message_queue(message_queue_type=args.queue)
       
    bench = Benchmark()
    bench.set_message_queue(message_queue)
    bench.run()