#!usr/bin/python

from confluent_kafka import Producer
import datetime
import time

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message delivered: {0}".format(msg.value()))

p = Producer({'bootstrap.servers': '16.0.2.9:9092'})

try:
    for var in xrange(1,10001):
        p.produce('loadbalance', value='for this number #{0}'
                  .format(var), callback=acked)
        time.sleep(1)
        p.poll(0.5)

except KeyboardInterrupt:
    pass

p.flush(30)
