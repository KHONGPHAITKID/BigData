# docker-compose up -d
# docker-compose ps
# docker-compose exec python-app bash
# python main.py

from benchmark.benchmark import Benchmark
from benchmark.stats import Stats
from message_queue.kafka import KafkaMessageQueue, KafkaTopicManager

broker = "localhost:9093"

# Configuration for Kafka (adjust as needed)
config = {
    "topic": "5parTopic",
    "partition": 5,
    "replication_factor": 1,
    "producer": {"bootstrap.servers": broker},
    "consumer": {
        "bootstrap.servers": broker,
        "group.id": "test_group",
        "auto.offset.reset": "earliest"
    }
}

if __name__ == "__main__":
    # Instantiate the benchmark and set the message queue
    stats = Stats()
    queue = KafkaMessageQueue()  # Pass config if required
    queue.set_stats(stats)

    topicManager = KafkaTopicManager(config["producer"]["bootstrap.servers"])
    topicManager.create_topic(topic_name=config["topic"], num_partitions=config["partition"], replication_factor=config["replication_factor"])
    
    bench = Benchmark()
    queue.connect(config)
    bench.set_message_queue(queue)
    
    print("Connected to Kafka")
    bench.run()