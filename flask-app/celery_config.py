from app import app
import os
from celery import Celery
from celery.bin import worker

REDIS_URL = os.getenv('REDIS_BROKER_URL', 'redis://localhost:6379/0')
app.config['CELERY_BROKER_URL'] = REDIS_URL 

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.conf.result_backend = REDIS_URL
celery.conf.broker_transport_options = {
    'max_retries': 3,
    'interval_start': 0,
    'interval_step': 0.2,
    'interval_max': 0.2,
}
