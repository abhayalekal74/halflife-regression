from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app.celery_config import celery
from datetime import datetime, time, timedelta
from celery.task.control import revoke
from celery.result import AsyncResult
import os
import redis


INFER_ONCE_IN = os.getenv('INFER_ONCE_IN', 1) * 60
REDIS_EXPIRY = os.getenv('REDIS_EXPIRY', 10) * 60
redisClient = None


def get_redis_client():
	global redisClient
	if not redisClient:
		#redisClient = redis.Redis(os.getenv('RATE_LIMITER_REDIS', "localhost"))
		redisClient = redis.Redis(os.getenv('RATE_LIMITER_REDIS', "redis-cache-node.sxlph4.0001.use1.cache.amazonaws.com"))
	return redisClient


def __run_inference(user_id, attempts_df, todays_attempts):
	entity_types = ['subject', 'chapter']
	for entity_type in entity_types:
		results = []
		if len(attempts_df) > 0:
			last_practiced_map = presenter.get_last_practiced(user_id, entity_type) if todays_attempts else None
			results = model_functions.run_inference(attempts_df, entity_type, last_practiced_map)
		presenter.write_to_hlr_index(user_id, results, todays_attempts, entity_type)
	

def get_attempts_and_run_inference(user_id, t_start, today_start):
	only_todays_attempts = t_start == today_start
	attempts_df = presenter.get_attempts_of_user(user_id, t_start)

	if not only_todays_attempts:
		prev_attempts = attempts_df[attempts_df['attempttime'] < today_start]	
		__run_inference(user_id, prev_attempts, False)
		__run_inference(user_id, attempts_df[attempts_df['attempttime'] >= today_start], True)
	else:
		__run_inference(user_id, attempts_df, True)

	print ("get_attempts_and_run_inference: userid: {}, attempts: {}, t_start: {}".format(user_id, len(attempts_df), t_start))


@celery.task
def update_last_practiced_before_today():
	presenter.update_last_practiced_before_today()
	

@celery.task
def infer_on_attempts(user_id):
	today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
	if not presenter.past_attempts_fetched(user_id):
		t_minus_x = datetime.now() - timedelta(days=model_functions.MAX_HL)
		start_time = int(t_minus_x.timestamp() * 1000)
	else:
		start_time = today_start_ms
	get_attempts_and_run_inference(user_id, start_time, today_start_ms)
	print ("Getting attempts for {}".format(user_id))


#If the user has not attempted any questions in x minutes, run the model
"""
@celery.task
def check_latest_activity(user_id):
	redis = get_redis_client()
	key = 'latest-attempt-' + user_id
	latest_attempt = redis.get(key)
	print ("Checking latest activity of {}: {}".format(user_id, latest_attempt))
	if not latest_attempt or (datetime.now().timestamp() - float(latest_attempt)) >= INFER_ONCE_IN:
		print ("latest_attempt of {} is {}, running model".format(user_id, latest_attempt))
		infer_on_attempts(user_id)
		redis.delete(key)
"""


def add_to_queue(user_id):
	redis = get_redis_client()
	next_run_key = 'next-run-' + user_id
	next_run = redis.get(next_run_key)
	current_time = datetime.now().timestamp()
	#redis.set('latest-attempt-' + user_id, current_time, ex=REDIS_EXPIRY)
	if next_run and current_time <= float(next_run):
		return
	infer_on_attempts.apply_async(args=[user_id], countdown=INFER_ONCE_IN)
	redis.set(next_run_key, current_time + INFER_ONCE_IN, ex=REDIS_EXPIRY)
