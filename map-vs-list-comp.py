import boto3
import json
import ujson
import pprint
from timeit import default_timer as timer


def list_it(meow):
    pprint.pprint("list")
    start = timer()
    result = []
    for item in meow:
        result.append(ujson.loads(item))
    end = timer()
    print(end - start)
    return result

def map_json(meow):
    pprint.pprint("map_json")
    start = timer()
    result = map(json.loads, meow)
    end = timer()
    print(end - start)
    return result

def map_ujson(meow):
    pprint.pprint("map_ujson")
    start = timer()
    result = map(ujson.loads, meow)
    end = timer()
    print(end - start)
    return result

if __name__ == "__main__":
    s3obj = boto3.resource('s3').Object(bucket_name='time-waits-for-no-man', key="1980-05-04")
    contents = s3obj.get()['Body'].read().decode()
    meow = contents.splitlines()
    list_it(meow)
    map_json(meow)
    map_ujson(meow)
    
