import get_attempts_data as presenter
import topic_hlr_train as model_functions
from datetime import datetime, time, timedelta


WEIGHTS_PATH = "saved_weights.csv"


errors = {
	"no_data": "No data"
}


def calculate_current_recall(hl, last_practiced_at):
	lag_in_ms = datetime.now().timestamp() * 1000 - last_practiced_at
	lag_in_days = model_functions.to_days(lag_in_ms)
	return model_functions.get_recall(hl, lag_in_days)


def get_attempts_and_run_inference(user_id, t):
	attempts_df = presenter.get_attempts_of_user(user_id, t)
	if len(attempts_df) == 0:
		return errors['no_data'] 
	results = model_functions.run_inference(attempts_df, WEIGHTS_PATH)
	return presenter.write_to_hlr_index(user_id, results)


def run_on_last_x_days_attempts(user_id, x = model_functions.MAX_HL):
	t_minus_x = datetime.now() - timedelta(days=x)
	t_minus_x_in_ms = int(t_minus_x.timestamp() * 1000)
	return get_attempts_and_run_inference(user_id, t_minus_x_in_ms)
	

def run_on_todays_attempts(user_id):
	if presenter.past_attempts_fetched(user_id):
		today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
		return get_attempts_and_run_inference(user_id, today_start_ms)
	else:
		return run_on_last_x_days_attempts(user_id)


def get_all_chapters_data(user_id):
	rows = presenter.get_all_chapters_for_user(user_id)
	response = dict()
	for row in rows:
		row = row._asdict()
		response[int(row['chapterid'])] = calculate_current_recall(row['hl'], row['last_practiced_at']) 
	return response


def get_chapter_data(user_id, chapter_id):
	result = presenter.get_chapter_for_user(user_id, chapter_id)
	if result:
		result = result._asdict()
		return calculate_current_recall(result['hl'], result['last_practiced_at'])
	else:
		return errors['no_data']


if __name__=='__main__':
	run_on_todays_attempts("b22b9a30-5c3a-11e7-9529-f35d12faee88")
	print(get_all_chapters_data("b22b9a30-5c3a-11e7-9529-f35d12faee88"))
	print(get_chapter_data("b22b9a30-5c3a-11e7-9529-f35d12faee88", 7151))
