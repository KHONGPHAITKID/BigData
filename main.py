# docker-compose up -d
# docker-compose ps
# docker-compose exec python-app bash
# python main.py

from benchmark.benchmark import Benchmark
from benchmark.stats import Stats
from message_queue.kafka import KafkaMessageQueue, KafkaTopicManager
import os

# Get broker address from environment variable, with fallback logic
# Use KAFKA_BROKER env var if set, otherwise determine based on environment
broker = os.environ.get('KAFKA_BROKER')
if not broker:
    # Logic to detect if running inside Docker
    is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
    broker = "kafka:9092" if is_docker else "localhost:9093"

print(f"Using Kafka broker: {broker}")

# Configuration for Kafka (adjust as needed)
config = {
    "kafka": {
        "topic": "test_topic_1",
        "partition": 5,
        "replication_factor": 1,
        "producer": {"bootstrap.servers": broker},
        "consumer": {
            "bootstrap.servers": broker,
            "group.id": "test_group",
            "auto.offset.reset": "earliest"
        }
    }
}

if __name__ == "__main__":
    # Instantiate the benchmark and set the message queue
    stats = Stats()
    queue = KafkaMessageQueue()  # Pass config if required
    queue.set_stats(stats)

    topicManager = KafkaTopicManager(config["kafka"]["producer"]["bootstrap.servers"])
    topicManager.create_topic(topic_name=config["kafka"]["topic"], num_partitions=config["kafka"]["partition"], replication_factor=config["kafka"]["replication_factor"])
    
    bench = Benchmark()
    bench.set_message_queue(queue)
    
    print("Connected to Kafka")
    bench.run(config)