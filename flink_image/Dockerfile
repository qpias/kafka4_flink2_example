FROM apache/flink:2.2.0-java21

# Switch to root user to install packages
USER root

# Install python3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-21-jdk && \
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64 && \
    # --break-system-packages is required for Flink 2.2.0 (based on Ubuntu 24.04+) 
    # to bypass PEP 668 ("externally managed environment") error.
    pip3 install apache-flink==2.2.0 --break-system-packages && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Switch back to flink user
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Switch back to flink user
USER flink
