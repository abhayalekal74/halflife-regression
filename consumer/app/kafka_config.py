import os

TOPIC = os.getenv('KAFKA_TOPIC', 'halflife-ML')
HOST = os.getenv('KAFKA_HOST', '172.30.0.157:9092')
PARTITIONS = int(os.getenv('KAFKA_PARTITIONS', 8))
GROUP_ID = os.getenv('KAFKA_GROUP', '1')
