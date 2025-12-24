# app/flink_datastream_job.py
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy

class JsonKeyedMapFunction(MapFunction):
    def map(self, value):
        data = json.loads(value)
        # Ensure 'message' key exists and uppercase it
        if "message" in data:
            original_message_upper = data["message"].upper()
            data["messages"] = json.dumps([original_message_upper]) # Store this message as a JSON string for accumulation
        else:
            data["messages"] = json.dumps([]) # Initialize as empty JSON list string if no message field
        
        # Initialize count for this message to "1"
        data["count"] = "1"
        
        # Return a tuple of (key, data) for key_by
        return (data.get("key", "default_key"), data)

class WindowCountReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        # value1 is the accumulated result (tuple: key, data_map)
        # value2 is the new element (tuple: key, data_map)

        # Count accumulation
        current_count = int(value1[1].get("count", "0"))
        new_element_count = int(value2[1].get("count", "0")) # This should be "1" from JsonKeyedMapFunction
        value1[1]["count"] = str(current_count + new_element_count)

        # Message accumulation: load, merge, then dump back to JSON string
        # Ensure 'messages' is always a JSON string before loading
        accumulated_messages_raw = value1[1].get("messages")
        if accumulated_messages_raw is None:
            accumulated_messages = []
        else:
            accumulated_messages = json.loads(accumulated_messages_raw)

        new_messages_raw = value2[1].get("messages")
        if new_messages_raw is None:
            new_messages = []
        else:
            new_messages = json.loads(new_messages_raw)
        
        for msg in new_messages:
            if msg not in accumulated_messages: # Avoid duplicates if present in both (unlikely for individual elements)
                accumulated_messages.append(msg)
        
        value1[1]["messages"] = json.dumps(accumulated_messages) # Store as JSON string
        return value1




def main():
    # Set up the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000)  # Enable checkpointing for fault tolerance

    # Migrated to KafkaSource (Flink 2.0+ standard) from legacy FlinkKafkaConsumer
    # Uses internal Docker listener: kafka:29092
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("input_topic") \
        .set_group_id("my_datastream_window_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Add Kafka source to the environment
    data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Apply the MapFunction to extract key and uppercase message
    keyed_stream = data_stream.map(JsonKeyedMapFunction(), output_type=Types.TUPLE([Types.STRING(), Types.MAP(Types.STRING(), Types.STRING())]))

    # Apply windowing and reduction
    windowed_stream = keyed_stream \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .reduce(WindowCountReduceFunction(), 
                output_type=Types.TUPLE([Types.STRING(), Types.MAP(Types.STRING(), Types.STRING())])) \
        .map(lambda x: {
            "key": x[0],
            "count": x[1].get("count", "0"), # Retrieve the final accumulated count from reduce function
            "messages": x[1].get("messages", "[]")
        }) \
        .map(lambda x: json.dumps(x), output_type=Types.STRING())


    # Migrated to KafkaSink (Flink 2.0+ standard) from legacy FlinkKafkaProducer
    # Uses internal Docker listener: kafka:29092
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("output_topic")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # Add Kafka sink to the environment
    windowed_stream.sink_to(kafka_sink)

    # Execute the Flink job
    env.execute("Flink DataStream Window Example")

if __name__ == '__main__':
    main()
