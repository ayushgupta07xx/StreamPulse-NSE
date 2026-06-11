# StreamPulse Flink image: Flink 1.18 + Python 3.10 + PyFlink + Kafka connector.
# PyFlink jobs run inside this image (cluster-pinned Python), not on the host.
FROM flink:1.18.1-scala_2.12-java11

# hadolint ignore=DL3008
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink pinned to the cluster version (supports Python 3.8-3.10)
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka SQL connector (bundles kafka-clients; serves DataStream + Table API)
# Prometheus metrics reporter already ships pre-activated in /opt/flink/plugins/.
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# Checkpoint dir pre-created with flink ownership so the named volume that
# mounts here inherits writable permissions (fresh volumes copy image perms).
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
