FROM python:3.12

ENV HOME /home/col
WORKDIR /home/col

# Install software dependencies
RUN apt-get update -y && apt-get install -y python3-pip python3-dev sqlite3

# Install python dependencies
RUN pip install --upgrade pip setuptools wheel
COPY requirements.txt /home/col/requirements.txt
RUN pip install -r /home/col/requirements.txt

COPY exporter.py /home/col/exporter.py
COPY schema.sql /home/col/schema.sql
