# Kafka and Flink End-to-End Example

This project aims to provide a comprehensive, working local development setup for Kafka and Flink, leveraging Docker and Docker Compose for easy deployment and management. It serves as a practical demonstration of various Flink API layers, complete with Python scripts to generate sample events and observe processed results. The project uses **Apache Kafka 4.1.1** (KRaft mode) and **Apache Flink 2.2.0**.

The data pipeline showcases:
1. A Kafka producer that sends messages to an `input_topic`.
2. Flink jobs that read messages from `input_topic`, process them using different API layers, and send the results to an `output_topic`.
3. A Kafka consumer that reads messages from `output_topic` and prints them to the console.

## Flink API Layers Demonstrated

This project showcases different levels of abstraction available in Apache Flink's Python API, demonstrating how to approach stream processing tasks with varying degrees of control and complexity:

*   **Table API (`flink_table_job.py`):** This is the highest-level API, offering a SQL-like or relational-style approach to stream processing. It's ideal for defining transformations on tabular data streams using declarative queries. The `flink_table_job.py` example uses the Table API to read from Kafka, apply a simple `UPPER` case conversion to a column, and write to another Kafka topic, much like a database query on a continuous stream.

*   **DataStream API (`flink_datastream_job.py`):** This is the core API for stream processing in Flink, providing more control over state and time compared to the Table API. It's suitable for complex event processing, windowing, and custom stateful operations. The `flink_datastream_job.py` example utilizes the DataStream API to implement windowing logic, where messages are grouped over a specific time period, and a custom `ReduceFunction` aggregates these messages and their counts.

*   **ProcessFunction API (`flink_process_function_job.py`):** This is Flink's low-level API, offering fine-grained control over streams, state, and timers. It allows you to process individual elements, query and update state, and register timers for event-time or processing-time based actions. The `flink_process_function_job.py` example demonstrates a `KeyedProcessFunction` to detect inactivity by using timers and managing keyed state for each incoming message, transforming them to uppercase and adding a processing timestamp.

These examples provide a comprehensive overview of Flink's capabilities, from high-level declarative processing to highly customized stateful and time-sensitive operations.

---

## Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop) and [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.x installed locally (for running the producer and consumer)
- `curl` (to download the Flink connector)

## Getting Started

### 1. Start Kafka and Flink Services

Run the following command to build the Flink image and start the cluster:
```bash
docker-compose up -d --build
```

Once the services are active, you can manage the cluster using either the provided Web UIs or the command line tools mentioned below:

*   **Flink Web UI** ([http://localhost:8081](http://localhost:8081)): Monitor job progress, view taskmanager logs, inspect execution graphs, and manually cancel jobs.
*   **Kafka UI** ([http://localhost:8080](http://localhost:8080)): Inspect message payloads in real-time, browse topics, and check consumer group lag without using CLI commands.

### 2. Create Kafka Topics

Create the input and output topics required for the exercise:
```bash
docker-compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic input_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic output_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Install Local Python Dependencies

Install the required Kafka Python client:
```bash
pip install kafka-python-ng
```

### 4. Download Flink Connector

Create a directory for the connector and download the required Kafka connector JAR for Flink 2.2.0:
```bash
mkdir -p app/lib
curl -o app/lib/flink-sql-connector-kafka-4.0.1-2.0.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
```

### 4. Run the application

The application consists of three parts: a producer, a Flink job, and a consumer.

#### a. Submit the Flink Table API job

Submit the Table API job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_table_job.py -j /app/lib/flink-sql-connector-kafka-4.0.1-2.0.jar
```

#### b. Submit the Flink DataStream API job

To run the DataStream API example, first ensure no other Flink job is running. `flink_datastream_job.py` uses the new `KafkaSource` and `KafkaSink` APIs.

Submit the job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_datastream_job.py -j /app/lib/flink-sql-connector-kafka-4.0.1-2.0.jar
```

#### c. Submit the Flink ProcessFunction job

To run the ProcessFunction example:

```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_process_function_job.py -j /app/lib/flink-sql-connector-kafka-4.0.1-2.0.jar
```

#### d. Run the consumer

Open a new terminal and run the consumer script. The consumer will wait for messages on the `output_topic`.
```bash
python3 app/consumer.py
```

#### e. Run the producer

Open another new terminal and run the producer script. This will send messages to the `input_topic` with a "key" for windowing.
```bash
python3 app/producer.py
```

You should see the original messages in the producer terminal and the processed results appearing in the consumer terminal as the Flink jobs transform the stream.

### 5. Managing Flink Jobs

You can monitor and manage your Flink jobs via the **Flink Web UI** or the command line.

**List all running jobs and their IDs:**
```bash
docker-compose exec flink-jobmanager ./bin/flink list
```

**Cancel a running job (immediate):**
```bash
docker-compose exec flink-jobmanager ./bin/flink cancel <JOB_ID>
```

**Stop a job gracefully (creating a savepoint):**
```bash
docker-compose exec flink-jobmanager ./bin/flink stop <JOB_ID>
```

**Cancel all running jobs:**
```bash
docker-compose exec flink-jobmanager /bin/bash -c "./bin/flink list -r | awk '{print \$4}' | xargs -I {} ./bin/flink cancel {}"
```

### 6. Managing Consumer Groups

In Kafka, consumers are always part of a **Consumer Group**. This allows multiple consumers to share the workload of processing a topic and ensures that messages are tracked correctly. The consumer script uses a stable group ID (`flink-example-consumer-group`) so it can resume from the last committed offset if restarted.

You can monitor group health via the **Kafka UI** or the CLI.

**List active consumer groups:**
```bash
docker-compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

**Describe a specific group (to see offsets and lag):**
```bash
docker-compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-example-consumer-group
```

**Delete unused (orphan) groups:**
```bash
docker-compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group <group-id-to-delete>
```

### 7. Clean up

To stop the services and remove the containers, run:
```bash
docker-compose down
```
