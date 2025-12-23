# Kafka and Flink End-to-End Example

This project aims to provide a comprehensive, working local development setup for Kafka and Flink, leveraging Docker and Docker Compose for easy deployment and management. It serves as a practical demonstration of various Flink API layers, complete with Python scripts to generate sample events and observe processed results.

The data pipeline showcases:
1. A Kafka producer that sends messages to an `input_topic`.
2. Flink jobs that read messages from `input_topic`, process them using different API layers, and send the results to an `output_topic`.
3. A Kafka consumer that reads messages from `output_topic` and prints them to the console.

## Flink API Layers Demonstrated

This project showcases different levels of abstraction available in Apache Flink's Python API, demonstrating how to approach stream processing tasks with varying degrees of control and complexity:

*   **Table API (`flink_job.py`):** This is the highest-level API, offering a SQL-like or relational-style approach to stream processing. It's ideal for defining transformations on tabular data streams using declarative queries. The `flink_job.py` example uses the Table API to read from Kafka, apply a simple `UPPER` case conversion to a column, and write to another Kafka topic, much like a database query on a continuous stream.

*   **DataStream API (`flink_datastream_job.py`):** This is the core API for stream processing in Flink, providing more control over state and time compared to the Table API. It's suitable for complex event processing, windowing, and custom stateful operations. The `flink_datastream_job.py` example utilizes the DataStream API to implement windowing logic, where messages are grouped over a specific time period, and a custom `ReduceFunction` aggregates these messages and their counts.

*   **ProcessFunction API (`flink_process_function_job.py`):** This is Flink's low-level API, offering fine-grained control over streams, state, and timers. It allows you to process individual elements, query and update state, and register timers for event-time or processing-time based actions. The `flink_process_function_job.py` example demonstrates a `KeyedProcessFunction` to detect inactivity by using timers and managing keyed state for each incoming message, transforming them to uppercase and adding a processing timestamp.

These examples provide a comprehensive overview of Flink's capabilities, from high-level declarative processing to highly customized stateful and time-sensitive operations.

## Prerequisites
- Docker and Docker Compose
- Python 3

## Instructions

### 1. Start the services

Start the ZooKeeper, Kafka, and Flink services using Docker Compose. This configuration uses ZooKeeper for Kafka coordination, which is a highly stable and reliable setup.
```bash
docker-compose up -d --build
```

### 2. Create Kafka topics

Create the `input_topic` and `output_topic` for the Kafka broker. We will connect to ZooKeeper to create the topics.

```bash
docker-compose exec kafka /bin/kafka-topics --create --topic input_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker-compose exec kafka /bin/kafka-topics --create --topic output_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Install Python dependencies

Install the required Python libraries. It is recommended to use a virtual environment.
```bash
pip3 install kafka-python apache-flink
```

### 4. Run the application

The application consists of three parts: a producer, a Flink job, and a consumer.

#### a. Submit the Flink Table API job

The Flink job will run in the background, waiting for messages. You need to submit the Python Flink job to the Flink cluster. To do this, you need the Flink Python connector jars.

**Note:** The producer and consumer scripts, along with the Flink job, have been updated to use JSON formatted messages. The producer sends JSON objects with a "message" field, and the consumer expects to parse these JSON objects.

First, download the necessary jar to your local `app/lib` directory:
```bash
mkdir -p app/lib
curl -o app/lib/flink-sql-connector-kafka-3.1.0-1.18.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
```

Now, submit the job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager flink run -py /app/flink_job.py -j /app/lib/flink-sql-connector-kafka-3.1.0-1.18.jar
```

This command will submit the job, and it will run indefinitely, processing data. You can view the running job in the Flink Web UI at [http://localhost:8081](http://localhost:8081).

#### b. Submit the Flink DataStream API job

To run the DataStream API example, first ensure no other Flink job is running. You can cancel a running job from the Flink Web UI (http://localhost:8081) or by restarting the Flink services (`docker-compose up -d --build --force-recreate`).

Then, submit the job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager flink run -py /app/flink_datastream_job.py -j /app/lib/flink-sql-connector-kafka-3.1.0-1.18.jar
```

#### c. Submit the Flink ProcessFunction job

To run the ProcessFunction example, first ensure no other Flink job is running. You can cancel a running job from the Flink Web UI (http://localhost:8081) or by restarting the Flink services (`docker-compose up -d --build --force-recreate`).

Then, submit the job using `docker-compose exec`:
```bash
docker-compose exec flink-jobmanager flink run -py /app/flink_process_function_job.py -j /app/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

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

You should see the original messages in the producer terminal and the aggregated window results in the consumer terminal when running the DataStream API job. For the Table API and ProcessFunction jobs, you will see the individual processed messages.

### 5. Clean up

To stop the services and remove the containers, run:
```bash
docker-compose down
```
