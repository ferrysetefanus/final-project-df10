FROM apache/airflow:2.0.2-python3.8
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install gdown