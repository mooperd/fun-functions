import boto3
import json
import ujson
import pprint
import arrow

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

def map_ujson(raw):
    raw = map(ujson.loads, meow)
    # result = map(add_timestamp, raw)
    return raw

if __name__ == "__main__":
    s3obj = boto3.resource('s3').Object(bucket_name='time-waits-for-no-man', key="1980-05-04")
    contents = s3obj.get()['Body'].read().decode()
    meow = contents.splitlines()
    output = map_ujson(meow)
    # a little bit to inspect the result:
    counter = 0
    limit = 10

    for wtf in output:
        pprint.pprint(wtf)
        counter +=1
        if counter == limit:
            exit()
