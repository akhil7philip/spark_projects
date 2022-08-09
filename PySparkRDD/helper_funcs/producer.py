import time
import json
from kafka import KafkaProducer
from data_generator import run

import logging
logger = logging.getLogger(__name__)

topic               = 'test_124' 
bootstrap_servers   = 'localhost:9092'

class Producer:
    def __init__(self, topic, bootstrap_servers):
        self.topic                  = topic
        self.bootstrap_servers      = bootstrap_servers
        self.producer               = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,\
            key_serializer=lambda x:json.dumps(x).encode('utf-8'),\
            value_serializer=lambda x:json.dumps(x).encode('utf-8'))

    def send_message(self, key=None, value=None):
        self.producer.send(self.topic, key=key, value=value)

    def flush_message(self):
        self.producer.flush()

if __name__ == '__main__':
    
    o   = Producer(topic, [bootstrap_servers])
    for v in run():
        print("producing message %s for topic %s", v, topic)
        o.send_message(
            key     = None,\
            value   = v)
        time.sleep(1)