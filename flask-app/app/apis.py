import sys
from flask import request, jsonify
from app import app
from app import get_attempts_data as presenter
from app import topic_hlr_train as model_functions
from app import kafka_producer 
from datetime import datetime
import os
from decimal import Decimal

errors = {
	"no_attempts_in_x_days": "No attempts in last {} days".format(model_functions.MAX_HL),
	"no_attempts_today": "No attempts today",
	"no_data": "No data"
}


def calculate_current_recall(hl, last_practiced_at, original_recall):
	lag_in_ms = datetime.now().timestamp() * 1000 - last_practiced_at
	lag_in_days = model_functions.to_days(lag_in_ms)
	# Multiplying by original recall because the recall calculated with hl and last_practiced_at is for original_recall of 1. But since we don't reset the recall to 1 after every session, we have to multiply it by original recall.
	current_recall = Decimal(model_functions.get_recall(hl, lag_in_days) * original_recall)
	return float(round(current_recall, 3))


@app.route('/recall/calculate', methods=['POST'])
def queue_calculate_recall_request():
	user_id = request.form['userid'] 
	kafka_producer.publish(user_id)
	return jsonify(success=True)


def get_latest_attempt_time(t1, t2):
	if not t1:
		return t2
	elif not t2:
		return t1
	else:
		return max(t1, t2)


@app.route('/recall/all_chapters', methods=['GET'])
def get_all_chapters_data():
	user_id = request.args['userid']
	rows = presenter.get_all_entities_for_user(user_id, 'chapter')
	data = _process_entities_data(rows)
	return jsonify(success=True, data=data)	


@app.route('/recall/all_subjects', methods=['GET'])
def get_all_subjects_data():
	user_id = request.args['userid']
	rows = presenter.get_all_entities_for_user(user_id, 'subject')
	data = _process_entities_data(rows)
	return jsonify(success=True, data=data)	


@app.route('/recall/all', methods=['GET'])
def get_all_entities_recall():
	user_id = request.args['userid']
	count = int(request.args.get('count', 20)) # number of strongest and weakest chapters to return
	chapters = presenter.get_all_entities_for_user(user_id, 'chapter')
	subjects = presenter.get_all_entities_for_user(user_id, 'subject')
	return jsonify(success=True, \
					chapters=_process_entities_data(chapters), \
					subjects=_process_entities_data(subjects))
	

def _process_entities_data(rows):
	response = dict()
	if rows:
		for row in rows:
			last_practiced_at = get_latest_attempt_time(row.last_practiced_before_today, row.last_practiced_today)
			response[int(row.entity_id)] = calculate_current_recall(row.hl, last_practiced_at, row.recall) 
	return response 


@app.route('/recall/chapter', methods=['GET'])
def get_chapter_data():
	user_id = request.args['userid']
	chapter_id = request.args['chapterid']
	result = presenter.get_entity_for_user(user_id, 'chapter' ,chapter_id)
	return process_entity_data(result)


@app.route('/recall/subject', methods=['GET'])
def get_subject_data():
	user_id = request.args['userid']
	subject_id = request.args['subjectid']
	result = presenter.get_entity_for_user(user_id, 'subject' ,subject_id)
	return process_entity_data(result)


def process_entity_data(result):
	if result:
		last_practiced_at = get_latest_attempt_time(result.last_practiced_before_today, result.last_practiced_today)
		return jsonify(success=True, recall=calculate_current_recall(result.hl, last_practiced_at, result.recall))
	else:
		return jsonify(success=False, error=errors['no_data'])


@app.route('/chapter/strongest', methods=['GET'])
def get_strongest_chapters():
	user_id = request.args['userid']
	count = int(request.args.get('count', 5))
	rows = presenter.get_all_entities_for_user(user_id, 'chapter')
	data = select_entities_in_order(rows, count, reverse=True) 
	return jsonify(data=data, success=True)


@app.route('/subject/strongest', methods=['GET'])
def get_strongest_subjects():
	user_id = request.args['userid']
	count = int(request.args.get('count', 5))
	rows = presenter.get_all_entities_for_user(user_id, 'subject')
	data = select_entities_in_order(rows, count, reverse=True) 
	return jsonify(data=data, success=True)


@app.route('/chapter/weakest', methods=['GET'])
def get_weakest_chapters():
	user_id = request.args['userid']
	count = int(request.args.get('count', 5))
	rows = presenter.get_all_entities_for_user(user_id, 'chapter')
	data = select_entities_in_order(rows, count) 
	return jsonify(data=data, success=True)


@app.route('/subject/weakest', methods=['GET'])
def get_weakest_subjects():
	user_id = request.args['userid']
	count = int(request.args.get('count', 5))
	rows = presenter.get_all_entities_for_user(user_id, 'subject')
	data = select_entities_in_order(rows, count) 
	return jsonify(data=data, success=True)


def select_entities_in_order(rows, count, reverse=False):
	data = list()
	if rows:
		for row in rows:
			last_practiced_at = get_latest_attempt_time(row.last_practiced_before_today, row.last_practiced_today)
			data.append([int(row.entity_id), calculate_current_recall(row.hl, last_practiced_at, row.recall)]) 
		data.sort(key=lambda x: x[1], reverse=reverse)
	return data[:count]


@app.route('/status') 
def return_status():
	return jsonify(status='OK')
