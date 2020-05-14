import sys
import json
from kafka import KafkaProducer, TopicPartition
from kafka.partitioner import RoundRobinPartitioner
from app import kafka_config

producer = None


def get_producer():
	global producer
	if producer is None:
		partitions = []
		for idx in range(kafka_config.CONSUMERS):
			partitions.append(TopicPartition(topic=kafka_config.TOPIC, partition=idx))
		partitioner = RoundRobinPartitioner(partitions=partitions)

		producer = KafkaProducer(bootstrap_servers=[kafka_config.HOST],
			key_serializer=lambda m: m.encode('utf8'),
			value_serializer=lambda m: json.dumps(m).encode('utf8'),
			partitioner=partitioner)
	return producer


def publish(userid):
	get_producer().send(kafka_config.TOPIC, key='key', value=dict(userid=userid))
