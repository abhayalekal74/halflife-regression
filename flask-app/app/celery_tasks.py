from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app.celery_config import celery
from datetime import datetime, time, timedelta
from celery.task.control import revoke
from celery.result import AsyncResult
import os
import redis


CHECK_INACTIVITY_AFTER = 60 # If no new request in these many seconds, run model
redisClient = None


def get_redis_client():
	global redisClient
	if not redisClient:
		#redisClient = redis.Redis(os.getenv('RATE_LIMITER_REDIS', "localhost"))
		redisClient = redis.Redis(os.getenv('RATE_LIMITER_REDIS', "redis-cache-node.sxlph4.0001.use1.cache.amazonaws.com"))
	return redisClient


@celery.task
def get_attempts_and_run_inference(user_id, t_start, t_end, todays_attempts):
	attempts_df = presenter.get_attempts_of_user(user_id, t_start, t_end)
	if len(attempts_df) > 0:
		entity_types = ['subject', 'chapter']
		for entity_type in entity_types:
			last_practiced_map = presenter.get_last_practiced(user_id, entity_type) if todays_attempts else None
			results = model_functions.run_inference(attempts_df, entity_type, last_practiced_map)
			presenter.write_to_hlr_index(user_id, results, todays_attempts, entity_type)
	print ("get_attempts_and_run_inference: userid: {}, attempts: {}, t_start: {}, t_end: {}".format(user_id, len(attempts_df), t_start, t_end))


@celery.task
def update_last_practiced_before_today():
	presenter.update_last_practiced_before_today()


# x in days
def infer_on_last_x_days_attempts(user_id, x = model_functions.MAX_HL, attempts_up_to=None):
	t_minus_x = datetime.now() - timedelta(days=x)
	t_minus_x_in_ms = int(t_minus_x.timestamp() * 1000)
	task = get_attempts_and_run_inference.apply_async(args=[user_id, t_minus_x_in_ms, attempts_up_to, False])
	print ("Task ID", task)
	

def infer_on_todays_attempts(user_id):
	today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
	task_delay = 0
	if not presenter.past_attempts_fetched(user_id):
		print ("Getting x days' attempts")
		infer_on_last_x_days_attempts(user_id, attempts_up_to=today_start_ms)
		task_delay = 120
	print ("Getting today's attempts, starting in {} seconds".format(task_delay))
	get_attempts_and_run_inference.apply_async(args=[user_id, today_start_ms, int(datetime.now().timestamp() * 1000), True], countdown=task_delay)


"""
If the user has not attempted any questions in x minutes, run the model
"""
@celery.task
def check_latest_activity(user_id):
	redis = get_redis_client()
	key = 'latest-attempt-' + user_id
	latest_attempt = redis.get(key)
	print ("latest_attempt of {} is {}".format(user_id, latest_attempt))
	if not latest_attempt or (datetime.now().timestamp() - float(latest_attempt)) >= CHECK_INACTIVITY_AFTER:
		infer_on_todays_attempts(user_id)
		redis.delete(key)


@celery.task
def add_to_queue(user_id):
	redis = get_redis_client()
	current_time = datetime.now().timestamp()
	print ("Adding {} to queue {}".format(user_id, current_time))
	newly_scheduled_task = check_latest_activity.apply_async(args=[user_id], countdown=CHECK_INACTIVITY_AFTER)
	redis.set('latest-attempt-' + user_id, current_time)
