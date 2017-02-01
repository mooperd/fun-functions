#!/usr/bin/env python
import threading, logging, time
import ujson
from kafka import KafkaConsumer, KafkaProducer
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while True:
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def add_timestamp(dict):
         dict['timestamp'] = arrow.get(
                             int(dict['year']),
                             int(dict['month']),
                             int(dict['day']),
                             int(dict['hour']),
                             int(dict['minute']),
                             int(dict['second'])
                             ).timestamp
         return dict

    def run(self):
	# Connect to Cassandra
        cluster = Cluster()
        cluster.connection_class = LibevConnection
        session = cluster.connect()
	# Connect to Kafka
        consumer = KafkaConsumer(bootstrap_servers='192.168.65.111:1026',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['time'])
        for message in consumer:
	    object = ujson.loads(message)
            object_timestamp = self.add_timestamp(object)
            object_timestamp_json = ujson.dumps(object_timestamp)
            session.execute("insert into dev.time JSON" + object_timestamp_json)
            print (message)


def main():
    threads = [ Consumer() ]

    for t in threads:
        t.start()

    time.sleep(100)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
