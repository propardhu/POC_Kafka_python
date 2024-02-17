from kafka import KafkaProducer
import time

# Configure the producer connection to the Kafka server
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def produce_messages():
    for _ in range(100):
        message = "This is a test message".encode('utf-8')
        producer.send('test_topic', message)
        time.sleep(0.1) # Sleep for 0.1 seconds to send 10 messages per second

if __name__ == "__main__":
    produce_messages()
    producer.flush() # Ensure all messages are sent before closing
    producer.close()
