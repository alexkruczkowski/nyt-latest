# Add necessary python packages 
FROM apache/airflow:2.1.0
FROM python:3.9.6-slim-buster
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt