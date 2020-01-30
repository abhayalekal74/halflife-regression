import get_attempts_data as presenter
import topic_hlr_train as model_functions
from datetime import datetime, time


WEIGHTS_PATH = "saved_weights.csv"


errors = {
	"no_data": "No new attempts"
}


def update(user_id):
	today_start_ms = int(datetime.combine(datetime.today(), time.min).timestamp() * 1000)
	attempts_df = presenter.get_attempts_of_user(user_id, today_start_ms)		
	if len(attempts_df) == 0:
		print (errors['no_data'])
		return errors['no_data'] 
	results = model_functions.run_inference(attempts_df, WEIGHTS_PATH)	
	for r in results:
		print (r)


if __name__=='__main__':
	update("b22b9a30-5c3a-11e7-9529-f35d12faee88")
