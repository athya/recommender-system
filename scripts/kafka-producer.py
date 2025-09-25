from kafka import KafkaConsumer, KafkaProducer
import time
import csv
import json

# Simulate stream of movie data

def read_csv(csv_file):
    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

KAFKA_TOPIC_NAME = "movielence"
KAFKA_BOOTSTRAP_SERVER_CONN = "kafka:9092"
try:
    kafka_producer_object = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER_CONN,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for movie in read_csv("movie.csv"):
        print("Message to be sent : ", movie)
        try:
            kafka_producer_object.send(KAFKA_TOPIC_NAME,movie)
        except Exception as e:
            print("Message not sent; ", e)
        time.sleep(1)
except Exception as e:
    print("Connection failed:", e)