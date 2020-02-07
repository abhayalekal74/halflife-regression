from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app.celery_config import celery
import os

WEIGHTS_PATH = os.path.join('app', 'saved_weights.csv')


@celery.task
def get_attempts_and_run_inference(user_id, t_start, t_end, entity_type, todays_attempts):
	attempts_df = presenter.get_attempts_of_user(user_id, t_start, t_end)
	if len(attempts_df) > 0:
		last_practiced_map = presenter.get_last_practiced(user_id, entity_type) if todays_attempts else None
		results = model_functions.run_inference(attempts_df, WEIGHTS_PATH, last_practiced_map)
		presenter.write_to_hlr_index(user_id, results, todays_attempts, entity_type)
	print ("get_attempts_and_run_inference: userid: {}, attempts: {}, t_start: {}, t_end: {}".format(user_id, len(attempts_df), t_start, t_end))


@celery.task
def update_last_practiced_before_today():
	presenter.update_last_practiced_before_today()
