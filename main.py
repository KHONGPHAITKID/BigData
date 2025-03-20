# docker-compose up -d
# docker-compose ps
# docker-compose exec python-app bash
# python main.py

from benchmark.benchmark import Benchmark
from message_queue.kafka import KafkaMessageQueue  # Replace with your message queue implementation

# Configuration for Kafka (adjust as needed)
config = {
    "topic": "bigdfamily",
    "producer": {"bootstrap.servers": "kafka:9092"},
    "consumer": {
        "bootstrap.servers": "kafka:9092",
        "group.id": "test_group",
        "auto.offset.reset": "earliest"
    }
}

if __name__ == "__main__":
    # Instantiate the benchmark and set the message queue
    bench = Benchmark()
    queue = KafkaMessageQueue()  # Pass config if required
    queue.connect(config)
    bench.set_message_queue(queue)
    print("Connected to Kafka")
    # Run the benchmark
    bench.run()
    print("Benchmark completed")
    stats = queue.get_stats()
    stats.print_log("latency", "latency.log")  # Save latency stats to a file
    stats.draw_histogram("latency", "latency_histogram.png")  # Create a histogram plot