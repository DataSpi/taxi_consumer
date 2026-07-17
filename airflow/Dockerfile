FROM apache/airflow:2.10.4-python3.11

# Spark/PySpark needs a JRE to run the JVM underneath it. default-jdk-headless
# sets up /usr/lib/jvm/default-java regardless of CPU arch (amd64 vs arm64),
# so we don't have to hardcode a version/arch-specific path.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r /requirements.txt
