import os

TOPIC = os.getenv('KAFKA_TOPIC', 'halflife-ML')
HOST = os.getenv('KAFKA_HOST', '172.30.0.157:9092')
CONSUMERS = int(os.getenv('KAFKA_CONSUMERS', 8))
