FROM apache/airflow:2.8.3

# Switch to root to install packages
USER root

# Install Java and procps (ps command)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set JAVA_HOME for Java applications
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install specific versions of Airflow and Spark provider
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3