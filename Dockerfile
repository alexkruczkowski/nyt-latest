# Add necessary python packages 
FROM apache/airflow:2.1.0
COPY requirements.txt /requirements.txt
USER airflow
RUN pip3 install --user --upgrade pip
RUN pip3 install --no-cache-dir --user -r /requirements.txt