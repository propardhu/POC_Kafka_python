from kafka import KafkaConsumer

# Configure the consumer connection to the Kafka server
consumer = KafkaConsumer(
    'topic_python_medium_demo',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='test-group'          # Consumer group ID
)

def consume_messages():
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")

if __name__ == "__main__":
    consume_messages()
