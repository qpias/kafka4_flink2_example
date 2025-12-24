FROM flink:2.0.0-java17

# Switch to root user to install packages
USER root

# Install python3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-17-jdk && \
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 && \
    pip3 install apache-flink==2.0.0 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Switch back to flink user
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Switch back to flink user
USER flink
