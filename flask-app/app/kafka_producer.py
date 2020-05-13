import sys
import json
from kafka import KafkaProducer, TopicPartition
from kafka.partitioner import RoundRobinPartitioner
from app import kafka_config

producer = None


def get_producer():
	global producer
	if producer is None:
		partitioner = RoundRobinPartitioner(partitions=[
			TopicPartition(topic=kafka_config.TOPIC, partition=0),
			TopicPartition(topic=kafka_config.TOPIC, partition=1),
			TopicPartition(topic=kafka_config.TOPIC, partition=2),
			TopicPartition(topic=kafka_config.TOPIC, partition=3)
		])

		producer = KafkaProducer(bootstrap_servers=[kafka_config.HOST],
			key_serializer=lambda m: m.encode('utf8'),
			value_serializer=lambda m: json.dumps(m).encode('utf8'),
			partitioner=partitioner)
	return producer


def publish(userid):
	print ("Publishing {}".format(userid), file=sys.stdout)
	get_producer().send(kafka_config.TOPIC, key='key', value=dict(userid=userid))
