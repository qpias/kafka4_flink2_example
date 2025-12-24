# Kafka and Flink End-to-End Example

This project aims to provide a comprehensive, working local development setup for Kafka and Flink, leveraging Docker and Docker Compose for easy deployment and management. It serves as a practical demonstration of various Flink API layers, complete with Python scripts to generate sample events and observe processed results. The project uses **Apache Kafka 4.0.0** (KRaft mode) and **Apache Flink 2.0.0**.

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

## Prerequisites
- Docker and Docker Compose
- Python 3

## Instructions

### 1. Start the services

Start the Kafka and Flink services using Docker Compose. Kafka 4.0 runs in KRaft mode, eliminating the need for ZooKeeper.
```bash
docker-compose up -d --build
```
Once the services are up, you can access:
- Flink Web UI: [http://localhost:8081](http://localhost:8081)
- Kafka UI: [http://localhost:8080](http://localhost:8080)

### 2. Create Kafka topics

Create the `input_topic` and `output_topic` for the Kafka broker.
```bash
docker-compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic input_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic output_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Install Python dependencies

Install the required Python libraries. It is recommended to use a virtual environment.
```bash
pip3 install kafka-python apache-flink
```

### 4. Download Flink Connector

Download the Flink Kafka connector JAR. This is required for the Flink jobs to communicate with Kafka.
```bash
mkdir -p app/lib
curl -o app/lib/flink-sql-connector-kafka-4.0.0-2.0.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar
```

### 4. Run the application

The application consists of three parts: a producer, a Flink job, and a consumer.

#### a. Submit the Flink Table API job

Submit the Table API job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_table_job.py -j /app/lib/flink-sql-connector-kafka-4.0.0-2.0.jar
```

#### b. Submit the Flink DataStream API job

To run the DataStream API example, first ensure no other Flink job is running. `flink_datastream_job.py` uses the new `KafkaSource` and `KafkaSink` APIs.

Submit the job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_datastream_job.py -j /app/lib/flink-sql-connector-kafka-4.0.0-2.0.jar
```

#### c. Submit the Flink ProcessFunction job

To run the ProcessFunction example:

```bash
docker-compose exec flink-jobmanager ./bin/flink run -py /app/flink_process_function_job.py -j /app/lib/flink-sql-connector-kafka-4.0.0-2.0.jar
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

You should see the original messages in the producer terminal and the aggregated window results in the consumer terminal when running the DataStream API job.

### 5. Clean up

To stop the services and remove the containers, run:
```bash
docker-compose down
```
