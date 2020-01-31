import get_attempts_data as presenter
import topic_hlr_train as model_functions
from datetime import datetime, time, timedelta


WEIGHTS_PATH = "saved_weights.csv"


errors = {
	"no_data": "No new attempts"
}


def calculate_current_recall(hl, last_practiced_at):
	lag_in_ms = datetime.now().timestamp() - last_practiced_at
	lag_in_days = model_functions.to_days(lag_in_ms)
	return model_functions.get_recall(hl, lag_in_days)


def update_db(results):
	pass


def get_all_chapters_data_for_user_from_db(user_id):
	pass


def get_chapter_data_for_user_from_db(user_id, chapter_id):
	pass


def get_attempts_and_run_inference(user_id, t):
	attempts_df = presenter.get_attempts_of_user(user_id, t)
	if len(attempts_df) == 0:
		return errors['no_data'] 
	results = model_functions.run_inference(attempts_df, WEIGHTS_PATH)
	update_db(results)
	return 1


def run_on_last_x_days_attempts(user_id, x = model_functions.MAX_HL):
	t_minus_x = datetime.now() - timedelta(days=x)
	t_minus_x_in_ms = int(t_minus_x.timestamp() * 1000)
	return get_attempts_and_run_inference(user_id, t_minus_x_in_ms)
	

def run_on_todays_attempts(user_id):
	today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
	return get_attempts_and_run_inference(user_id, today_start_ms)


def get_all_chapters_data(user_id):
	rows = get_all_chapters_data_for_user_from_db(user_id)
	response = dict()
	for row in rows:
		response[int(row['chapterid'])] = calculate_current_recall(row['hl'], row['last_practiced_at']) 
	return response


def get_chapter_data(user_id, chapter_id):
	row = get_chapter_data_for_user_from_db(user_id, chapter_id)
	return calculate_current_recall(row['hl', row['last_practiced_at'])


if __name__=='__main__':
	run_on_todays_attempts("b22b9a30-5c3a-11e7-9529-f35d12faee88")
