FROM python:3.7 

RUN apt-get update && apt-get install -y libatlas-base-dev gfortran nginx supervisor build-essential tcl redis-server

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

RUN useradd --no-create-home nginx

RUN rm /etc/nginx/sites-enabled/default

COPY nginx.conf /etc/nginx/
COPY flask-site-nginx.conf /etc/nginx/conf.d/
COPY uwsgi.ini /etc/uwsgi/
COPY supervisord.conf /etc/

COPY . .

CMD ["/usr/bin/supervisord"]
