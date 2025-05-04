FROM apache/airflow:latest

USER 0

RUN apt-get update && \
    apt-get -y install --no-install-recommends git default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
