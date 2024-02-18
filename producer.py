from kafka import KafkaProducer
import time

# Configure the producer connection to the Kafka server
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def produce_messages():
    count = 1
    for _ in range(1000):
        message = "This is a test message number " +str(count)
        message = message.encode('utf-8')
        count = count +1
        producer.send('topic_python_medium_demo', message)
        time.sleep(0.1)

if __name__ == "__main__":
    produce_messages()
    producer.flush()
    producer.close()
