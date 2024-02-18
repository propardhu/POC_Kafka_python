from confluent_kafka import Producer
import time

# Configuration for Kafka Producer
config = {
    'bootstrap.servers': 'localhost:9092'
}

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced: {msg.value()}")

producer = Producer(**config)

def produce_messages():
    for i in range(1000):
        message = f"This is a test message {i}".encode('utf-8')
        producer.produce('topic_python_medium_demo', value=message, callback=acked)
        # Wait up to 0.1 seconds for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
        producer.poll(0.1)

if __name__ == "__main__":
    produce_messages()
    # Wait for any outstanding messages to be delivered and invoke callbacks
    producer.flush()

