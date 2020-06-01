from flask import Flask

app = Flask(__name__)

from app import apis
from app import kafka_consumer
import threading

threading.Thread(target=kafka_consumer.start_consumer).start()
