# ===== Base OS =====
FROM debian:11

# ===== Versions =====
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3

# ===== Environment =====
ENV SPARK_VERSION=${SPARK_VERSION} \
    HADOOP_VERSION=${HADOOP_VERSION} \
    SPARK_HOME=/opt/bitnami/spark \
    PATH=/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:$PATH \
    LANG=C.UTF-8

# ===== Install dependencies =====
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    python3 \
    python3-pip \
    tini \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ===== Install Spark =====
RUN mkdir -p /opt/bitnami && \
    cd /opt/bitnami && \
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# ===== Create Spark user =====
RUN useradd -m -s /bin/bash spark && \
    chown -R spark:spark /opt/bitnami

USER spark
WORKDIR /opt/bitnami/spark

# ===== Python (optional but safe) =====
RUN pip install --no-cache-dir findspark

# ===== Entrypoint =====
RUN mkdir -p /opt/bitnami/scripts/spark && \
    cat << 'EOF' > /opt/bitnami/scripts/spark/entrypoint.sh
#!/bin/bash
set -e

SPARK_MODE=${SPARK_MODE:-master}
SPARK_HOME=${SPARK_HOME:-/opt/bitnami/spark}

echo "Starting Apache Spark in ${SPARK_MODE} mode..."

if [ "$SPARK_MODE" = "master" ]; then
  $SPARK_HOME/sbin/start-master.sh -h 0.0.0.0
elif [ "$SPARK_MODE" = "worker" ]; then
  if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERROR: SPARK_MASTER_URL not set"
    exit 1
  fi
  $SPARK_HOME/sbin/start-worker.sh "$SPARK_MASTER_URL"
elif [ "$SPARK_MODE" = "thrift" ]; then
  echo "Starting Spark Thrift Server..."
  $SPARK_HOME/sbin/start-thriftserver.sh \
    --master spark://${SPARK_MASTER_URL:-spark-master:7077} \
    --conf spark.sql.hive.thriftServer.singleSession=true \
    --conf spark.sql.adaptive.enabled=true
else
  exec "$@"
fi

tail -F $SPARK_HOME/logs/*.out
EOF

RUN chmod +x /opt/bitnami/scripts/spark/entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD ["spark-shell"]
