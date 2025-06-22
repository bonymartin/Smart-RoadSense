from confluent_kafka import Consumer, KafkaException

def consume_kafka_topic(broker, group_id, topic):
    # Kafka consumer configuration
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'  # Start from the earliest message if no offset is stored
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

        while True:
            msg = consumer.poll(timeout=1.0)  # Timeout in seconds

            if msg is None:
                # No message received
                continue
            if msg.error():
                # Handle error
                raise KafkaException(msg.error())
            else:
                # Proper message received
                print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Aborted by user")

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    broker = '192.168.8.29:29092'  # Kafka broker address
    group_id = 'my_group'
    topic = 'pothole_severity_type_distribution_counts'

    consume_kafka_topic(broker, group_id, topic)
