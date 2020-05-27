from flask import request, jsonify
from app import app
from app import kafka_producer 


@app.route('/recall/calculate', methods=['POST'])
def queue_calculate_recall_request():
	user_id = request.form['userid'] 
	kafka_producer.publish(user_id)
	return jsonify(success=True)

