FROM python:3.11-slim

ARG SPARK_VERSION=3.5.5
ARG HADOOP_VERSION=3
ARG USER_UID=1000
ARG USER_GID=1000
ARG APP_DIR=/workspaces/goit-de-hw-06

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates openjdk-21-jre procps tini git \
    iproute2 iputils-ping net-tools netcat-openbsd \
    build-essential gcc \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt \
    && curl -fsSL ${SPARK_URL} -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV PYSPARK_PYTHON=python3

RUN groupadd -g ${USER_GID} spark || true && \
    useradd -m -u ${USER_UID} -g ${USER_GID} spark || true

RUN mkdir -p ${APP_DIR}
WORKDIR ${APP_DIR}

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    if [ -f requirements.txt ]; then pip install -r requirements.txt; fi || true

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} jupyterlab black pytest kafka-python rich

# RUN apt-get purge -y build-essential gcc && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

USER spark

EXPOSE 4040 7077 8080 8888
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]