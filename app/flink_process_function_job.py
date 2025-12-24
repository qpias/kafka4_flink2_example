# app/flink_process_function_job.py
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction
from pyflink.common.time import Time
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.watermark_strategy import WatermarkStrategy

# Define the inactivity timeout duration (e.g., 5 seconds)
INACTIVITY_TIMEOUT_MS = 5000

class TimestampAndUppercaseProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context):
        # State to store the last seen processing time for each key
        self.last_seen_time_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_seen_time",  # The name of the state
                Types.LONG()       # The type of the state (timestamp in milliseconds)
            )
        )
        # State to store the last registered timer for each key
        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_timer",      # The name of the state
                Types.LONG()       # The type of the state (timestamp in milliseconds)
            )
        )

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        key = ctx.get_current_key() # Get the current key directly from ctx

        # Deserialize the incoming JSON string
        data = json.loads(value[1]) # Extract the message string from the tuple before parsing
        
        # Get the current processing time
        current_time = ctx.timer_service().current_processing_time()
        
        # Add a timestamp and convert the message to uppercase
        data["processing_time"] = current_time
        if "message" in data:
            data["message"] = data["message"].upper()
            
        # Emit the processed message
        yield json.dumps(data)

        # Update last seen time and register a new timer
        self.last_seen_time_state.update(current_time)

        # Clear previous timer if it exists
        last_timer = self.last_timer_state.value()
        if last_timer is not None:
            ctx.timer_service().delete_processing_time_timer(last_timer)

        # Register a new timer for inactivity detection
        new_timer_time = current_time + INACTIVITY_TIMEOUT_MS
        ctx.timer_service().register_processing_time_timer(new_timer_time)
        self.last_timer_state.update(new_timer_time)

    def on_timer(self, timestamp: int, ctx: 'ProcessFunction.Context'):
        key = ctx.get_current_key() # Get the current key directly from ctx
        last_seen_time = self.last_seen_time_state.value()
        last_timer_registered = self.last_timer_state.value()

        # Check if this timer is the last one registered and if no new message has arrived
        if last_timer_registered is not None and timestamp == last_timer_registered and timestamp == last_seen_time + INACTIVITY_TIMEOUT_MS:
            timeout_message = {
                "key": key,
                "type": "INACTIVITY_TIMEOUT",
                "timeout_time": timestamp,
                "last_message_time": last_seen_time
            }
            yield json.dumps(timeout_message)
        
        # Clear the timer state after it fires
        self.last_timer_state.clear()


def main():
    # Set up the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000)  # Enable checkpointing for fault tolerance

    # Migrated to KafkaSource (Flink 2.0+ standard) from legacy FlinkKafkaConsumer
    # Uses internal Docker listener: kafka:29092
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("input_topic") \
        .set_group_id("my_process_function_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Add Kafka source to the environment
    data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Key the stream by the 'key' field for ProcessFunction state
    keyed_stream = data_stream \
        .map(lambda x: (json.loads(x).get("key", "default_key"), x), output_type=Types.TUPLE([Types.STRING(), Types.STRING()])) \
        .key_by(lambda x: x[0])

    # Apply the ProcessFunction for inactivity detection
    processed_stream = keyed_stream.process(TimestampAndUppercaseProcessFunction(), output_type=Types.STRING()) # Output type is String

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
    processed_stream.sink_to(kafka_sink)

    # Execute the Flink job
    env.execute("Flink ProcessFunction Inactivity Detector Example")

if __name__ == '__main__':
    main()