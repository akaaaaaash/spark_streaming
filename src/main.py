from kafka_streaming import KafkaStream
from hdfs_operations import HDFSOperations
from data_validation import DataValidator
import logging

def main():
    try:
        # Initialize Kafka Stream
        bootstrap_servers = "your_kafka_bootstrap_servers"
        topic = "your_kafka_topic"
        checkpoint_dir = "/path/to/checkpoint/dir"
        kafka_stream = KafkaStream(bootstrap_servers, topic, checkpoint_dir)
    except Exception as e:
        logging.error("Failed to initialize Kafka stream: %s", e)
        return

    try:
        # Consume data from Kafka
        df = kafka_stream.consume_data()
    except Exception as e:
        logging.error("Failed to consume data from Kafka: %s", e)
        return

    try:
        # Initialize DataValidator and validate data
        validator = DataValidator()
        validated_df = validator.apply_data_validation(df)
    except Exception as e:
        logging.error("Data validation failed: %s", e)
        return

    try:
        # Initialize HDFS operations
        raw_zone_path = "/path/to/hdfs/raw_zone"
        processed_zone_path = "/path/to/hdfs/processed_zone"
        hdfs_ops = HDFSOperations(raw_zone_path, processed_zone_path)
    except Exception as e:
        logging.error("Failed to initialize HDFS operations: %s", e)
        return

    try:
        # Write validated data to HDFS
        hdfs_ops.write_to_hdfs(validated_df, "processed", partition_by="date_field")
    except Exception as e:
        logging.error("Failed to write data to HDFS: %s", e)
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)  # Configure logging
    main()
