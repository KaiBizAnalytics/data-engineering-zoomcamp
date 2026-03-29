FROM bitnami/spark:3.5

USER root

# Download GCS connector for Hadoop 3
RUN curl -fsSL \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar

RUN pip install --no-cache-dir google-cloud-storage==2.18.2

USER 1001

COPY transform.py /app/transform.py

ENTRYPOINT ["spark-submit", \
    "--conf", "spark.hadoop.google.cloud.auth.service.account.enable=true", \
    "/app/transform.py"]
