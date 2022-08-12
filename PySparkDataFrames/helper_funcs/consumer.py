from kafka import KafkaConsumer
import json

topic               = 'test_159'
bootstrap_servers   = 'localhost:9092'

if __name__ == "__main__":
    consumer = KafkaConsumer(
        topic,                                  # topic
        bootstrap_servers=bootstrap_servers,    # kafka server
        auto_offset_reset='earliest',           # 'earliest' consume from beginning;
                                                # else 'latest' (default) from last offset consumed
        group_id="consumer-group",              # group id
        )            

    print("starting the consumer")
    for msg in consumer:
        print(json.loads(msg.value))            # parse serialised values
