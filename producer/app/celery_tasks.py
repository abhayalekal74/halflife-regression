from app import app
from app import presenter
from app.celery_config import celery


@celery.task
def update_last_practiced_before_today():
	presenter.update_last_practiced_before_today()
