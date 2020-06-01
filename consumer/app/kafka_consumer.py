import sys
from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app import kafka_config
import json
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from datetime import datetime, time, timedelta
from collections import defaultdict


def __run_inference(user_id, attempts_df, todays_attempts):
	results = None 
	entity_types = ['subject', 'chapter']
	if len(attempts_df) > 0:
		last_practiced_map = presenter.get_last_practiced(user_id) if todays_attempts else defaultdict(list) 
		results = model_functions.run_inference(attempts_df, entity_types, last_practiced_map)
	presenter.write_to_hlr_index(user_id, results, todays_attempts, entity_types)
	

def get_attempts_and_run_inference(user_id, t_start, today_start):
	only_todays_attempts = t_start == today_start
	attempts_df = presenter.get_attempts_of_user(user_id, t_start)

	if len(attempts_df) == 0:
		if not only_todays_attempts:
			__run_inference(user_id, attempts_df, False)
		return

	if not only_todays_attempts:
		prev_attempts = attempts_df[attempts_df['attempttime'] < today_start]	
		__run_inference(user_id, prev_attempts, False)
		__run_inference(user_id, attempts_df[attempts_df['attempttime'] >= today_start], True)
	else:
		__run_inference(user_id, attempts_df, True)


def infer_on_attempts(user_id):
	today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
	if not presenter.past_attempts_fetched(user_id):
		t_minus_x = datetime.now() - timedelta(days=model_functions.MAX_HL)
		start_time = int(t_minus_x.timestamp() * 1000)
	else:
		start_time = today_start_ms
	get_attempts_and_run_inference(user_id, start_time, today_start_ms)


def start_consumer():
	print ("Starting consumer...")
	consumer = KafkaConsumer(bootstrap_servers=[kafka_config.HOST],
					key_deserializer=lambda m: m.decode('utf8'),
					value_deserializer=lambda m: json.loads(m.decode('utf8')),
					auto_offset_reset="latest",
					group_id=kafka_config.GROUP_ID)

	consumer.subscribe([kafka_config.TOPIC])

	for msg in consumer:
		infer_on_attempts(msg.value['userid'])
		print ("Consumer: {}".format(msg), file=sys.stdout)
		tp = TopicPartition(msg.topic, msg.partition)
		offsets = {tp: OffsetAndMetadata(msg.offset, None)}
		consumer.commit(offsets=offsets)
