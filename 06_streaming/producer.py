import json
import time
from kafka import KafkaProducer
import pandas as pd
from pandas import NaT, Timestamp

def json_serializer(data):
    return json.dumps(data, default=convert_missing_nat).encode('utf8')

def convert_missing_nat(obj):
    if obj is NaT:
        return None
    elif obj is Timestamp:
        return obj.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(obj) else None 
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

server = 'localhost:9092'
producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=json_serializer
)

# print(producer.bootstrap_connected())ß