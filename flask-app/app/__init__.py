from flask import Flask

app = Flask(__name__)

from app import apis

from app.celery_config import start_worker_process
start_worker_process()
