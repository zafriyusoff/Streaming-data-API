#!usr/bin/python

from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process
import time
import os

conf = {
    'bootstrap.servers': '16.0.2.9:9092',
    'group.id': 'loadbalance_grp',
    'client.id': 'loadbalance_client',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

topic = 'loadbalance'

def run():
    c = Consumer(**conf)
    c.subscribe([topic])
    try:
        while True:
              msg = c.poll(1.0)
              if msg is None:
                  continue
              elif not msg.error():
                  print('Received message: {0}'.format(msg.value()))
              elif msg.error().code() == KafkaError._PARTITION_EOF:
                  print('End of partition reached {0}/{1}'
                    .format(msg.topic(), msg.partition()))
              else:
                  print('Error occured: {0}'.format(msg.error().str()))
                  time.sleep(5)

    except KeyboardInterrupt:
        pass
    c.close()

workers = []
for _ in range(1):
    w = Process(target=run)
    w.start()
    workers.append(w)
time.sleep(5)

w = Process(target=run)
w.start()
workers.append(w)
time.sleep(5)

w = Process(target=run)
w.start()
workers.append(w)

for w in workers:
    w.join()
