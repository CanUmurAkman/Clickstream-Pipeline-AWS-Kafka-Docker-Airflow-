FROM apache/airflow:2.9.2
USER root
RUN apt-get update && apt-get install -y curl unzip \
 && curl -sS https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
 && unzip -q awscliv2.zip \
 && ./aws/install \
 && rm -rf aws awscliv2.zip \
 && python -m pip install --no-cache-dir boto3
USER airflow
