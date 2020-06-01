FROM python:3.7 

RUN apt-get update && apt-get install -y libatlas-base-dev gfortran supervisor 

RUN adduser --system --group hlr

USER hlr

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

WORKDIR /home/hlr/

COPY . . 

CMD ["/usr/bin/supervisord"]
