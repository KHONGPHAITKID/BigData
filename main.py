
from message_queue.rabbitmq import RabbitMQ
from message_queue.interface import Message
from benchmark.benchmark import Benchmark


if __name__ == "__main__":
    bench = Benchmark()
    rabbitmq = RabbitMQ(config={
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'queue': 'hello'
    })
    rabbitmq.connect()
    bench.set_message_queue(rabbitmq)
    bench.run()
    rabbitmq.close()
