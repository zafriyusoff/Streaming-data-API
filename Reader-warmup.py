#!usr/bin/python

from confluent_kafka import Consumer, KafkaError
import random
import time
import datetime

settings = {
    'bootstrap.servers': '16.0.2.9:9092',
    'group.id': 'group-1',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)

c.subscribe(['warmup'])

try:
    while True:
        msg = c.poll(1.0)
        rand = random.randint(10,15)
        time.sleep(rand)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

        print(datetime.datetime.now())

except KeyboardInterrupt:
    pass

finally:
   c.close()
