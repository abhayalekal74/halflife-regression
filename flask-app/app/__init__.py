from flask import Flask

app = Flask(__name__)

from app import apis
from app import celery_config 
from app import kafka_consumer

kafka_consumer.spawn_consumers()
