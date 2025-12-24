# app/flink_table_job.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    
    # Define the uppercase UDF
    @udf(result_type=DataTypes.STRING())
    def to_upper(data):
        return data.upper()

    # Create Kafka source table
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            `message` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'input_topic',
            'properties.bootstrap.servers' = 'kafka:29092', -- Internal Docker listener
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Create Kafka sink table
    t_env.execute_sql("""
        CREATE TABLE kafka_sink (
            `message` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'output_topic',
            'properties.bootstrap.servers' = 'kafka:29092', -- Internal Docker listener
            'format' = 'json'
        )
    """)

    # Create a table from the source
    source_table = t_env.from_path("kafka_source")

    # Apply the UDF and select the result
    result_table = source_table.select(to_upper(source_table.message))

    # Insert the result into the sink
    result_table.execute_insert("kafka_sink").wait()

if __name__ == '__main__':
    main()
