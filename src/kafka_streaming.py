from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

class KafkaStream:
    def __init__(self, bootstrap_servers, topic, checkpoint_dir):
        try:
            self.bootstrap_servers = bootstrap_servers
            self.topic = topic
            self.checkpoint_dir = checkpoint_dir
            self.spark = SparkSession.builder.appName("KafkaXMLStreaming").getOrCreate()
        except Exception as e:
            logging.error("Failed to initialize Spark Session: %s", e)
            raise

    def consume_data(self):
        try:
            kafka_schema = StructType([
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("value", StringType()),
                StructField("date_field", StringType())  # Assuming there is a date field
            ])
        except Exception as e:
            logging.error("Schema definition failed: %s", e)
            raise

        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()
        except Exception as e:
            logging.error("Failed to set up Kafka readStream: %s", e)
            raise

        try:
            value_df = df.selectExpr("CAST(value AS STRING)")
            parsed_df = value_df.select(from_json(col("value"), kafka_schema).alias("data")).select("data.*")
        except Exception as e:
            logging.error("Data parsing failed: %s", e)
            raise

        return parsed_df
