from app import app
from multiprocessing import Process
from celery.bin import worker


def start_celery_worker(*args):
	celery_worker = worker.worker(app=args[0])
	options = {
			'loglevel': 'INFO',
			'traceback': True
		}
	celery_worker.run(**options)


def run_app():
	app.run(host='0.0.0.0')


if __name__=='__main__':
	from celery_config import celery
	celery_process = Process(target=start_celery_worker, args=(celery, ))
	app_process = Process(target=run_app)
	celery_process.start()
	app_process.start()
