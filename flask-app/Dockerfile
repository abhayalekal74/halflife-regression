FROM python:3.7 
copy . .
RUN pip3 install -r requirements.txt
CMD sh configure_redis.sh
CMD sh configure_cassandra.sh
CMD python3 run.py
