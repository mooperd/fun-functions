#!/bin/python
import arrow
import pprint
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

dir      = "files"
start    = int(0)
stop    = int(100000)
#stop     = int(10000000)
#stop     = int(1474925221)

running = start

file_ = open("file", 'wb')


producer = KafkaProducer(bootstrap_servers=['192.168.65.111:1026'])

# future = producer.send('my-topic', b'raw_bytes')

while running < stop:
    
    dict = {}
    running = running + 1
    arrow_date = arrow.get(running)
    dict['year'] = arrow_date.format('YYYY')
    dict['month'] = arrow_date.format('MM')
    dict['day'] = arrow_date.format('DD')
    dict['hour'] = arrow_date.format('HH')
    dict['minute'] = arrow_date.format('mm')
    dict['second'] = arrow_date.format('ss')
    dict['timezone'] = arrow_date.format('ZZ')
    # create a new file handle when hour is zero 
    if dict['hour'] == "00" and dict['minute'] == "00" and dict['second'] == "00":
        filename = dir + "/" + dict['year'] + "-" + dict['month'] + "-" + dict['day']
        pprint.pprint(filename)
        file_ = open(filename, 'wb')
    # pprint.pprint(json.dumps(list))
    # pprint.pprint(file)
    future = producer.send('time', json.dumps(dict))
    file_.write("%s\n" % json.dumps(dict))

exit()
