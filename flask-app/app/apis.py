from flask import request, jsonify
from app import app
from celery import Celery
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from datetime import datetime, time, timedelta
import os


WEIGHTS_PATH = os.path.join('app', 'saved_weights.csv')


errors = {
	"no_attempts_in_x_days": "No attempts in last {} days".format(model_functions.MAX_HL),
	"no_attempts_today": "No attempts today",
	"no_data": "No data"
}


def calculate_current_recall(hl, last_practiced_at, original_recall):
	lag_in_ms = datetime.now().timestamp() * 1000 - last_practiced_at
	lag_in_days = model_functions.to_days(lag_in_ms)
	# Multiplying by original recall because the recall calculated with hl and last_practiced_at is for original_recall of 1. But since we don't reset the recall to 1 after every session, we have to multiply it by original recall.
	return model_functions.get_recall(hl, lag_in_days) * original_recall


def get_attempts_and_run_inference(user_id, t):
	attempts_df = presenter.get_attempts_of_user(user_id, t)
	if len(attempts_df) > 0:
		print ("Attempts", len(attempts_df))
		results = model_functions.run_inference(attempts_df, WEIGHTS_PATH)
		presenter.write_to_hlr_index(user_id, results)
	return len(attempts_df) > 0


def run_on_last_x_days_attempts(user_id, x = model_functions.MAX_HL):
	t_minus_x = datetime.now() - timedelta(days=x)
	t_minus_x_in_ms = int(t_minus_x.timestamp() * 1000)
	return get_attempts_and_run_inference(user_id, t_minus_x_in_ms)
	

@app.route('/recall/calculate', methods=['POST'])
def run_on_todays_attempts():
	user_id = request.form['userid'] 
	attempts_fetched = False
	if presenter.past_attempts_fetched(user_id):
		print ("Getting today's attempts")
		today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
		attempts_fetched = get_attempts_and_run_inference(user_id, today_start_ms)
	else:
		print ("Getting x days' attempts")
		attempts_fetched = run_on_last_x_days_attempts(user_id)
	return jsonify(success=True) if attempts_fetched else jsonify(success=False, error=errors['no_attempts_today'])


@app.route('/recall/all', methods=['GET'])
def get_all_chapters_data():
	user_id = request.args['userid']
	rows = presenter.get_all_chapters_for_user(user_id)
	response = dict()
	for row in rows:
		row = row._asdict()
		response[int(row['chapterid'])] = calculate_current_recall(row['hl'], row['last_practiced_at'], row['recall']) 
	if response:
		return jsonify(success=True, data=response)
	else:
		return jsonify(success=False, error=errors['no_data'])


@app.route('/recall/chapter', methods=['GET'])
def get_chapter_data():
	user_id = request.args['userid']
	chapter_id = request.args['chapterid']
	result = presenter.get_chapter_for_user(user_id, chapter_id)
	if result:
		result = result._asdict()
		return jsonify(success=True, recall=calculate_current_recall(result['hl'], result['last_practiced_at'], result['recall']))
	else:
		return jsonify(success=False, error=errors['no_data'])
