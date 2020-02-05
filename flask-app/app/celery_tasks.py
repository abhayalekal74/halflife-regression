from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app.celery_config import celery
import os

WEIGHTS_PATH = os.path.join('app', 'saved_weights.csv')


@celery.task
def get_attempts_and_run_inference(user_id, t):
	attempts_df = presenter.get_attempts_of_user(user_id, t)
	if len(attempts_df) > 0:
		results = model_functions.run_inference(attempts_df, WEIGHTS_PATH)
		presenter.write_to_hlr_index(user_id, results)
	print ("get_attempts_and_run_inference: userid: {}, attempts: {}, ts: {}".format(user_id, len(attempts_df), t))

