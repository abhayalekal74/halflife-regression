from flask import Flask

app = Flask(__name__)

from app import apis
from app import celery_tasks 
