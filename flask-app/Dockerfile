FROM python:3.7 

RUN apt-get update && apt-get install -y libatlas-base-dev gfortran supervisor

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY supervisord.conf /etc/

COPY . .

CMD ["/usr/bin/supervisord"]
