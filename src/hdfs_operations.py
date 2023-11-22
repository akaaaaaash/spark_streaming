from pyspark.sql import SparkSession
import logging

class HDFSOperations:
    def __init__(self, raw_zone_path, processed_zone_path):
        try:
            self.raw_zone_path = raw_zone_path
            self.processed_zone_path = processed_zone_path
            self.spark = SparkSession.builder.appName("HDFSOperations").getOrCreate()
        except Exception as e:
            logging.error("Failed to initialize Spark Session for HDFS operations: %s", e)
            raise

    def write_to_hdfs(self, dataframe, zone, partition_by=None):
        try:
            path = self.raw_zone_path if zone == "raw" else self.processed_zone_path
            write_df = dataframe if partition_by is None else dataframe.partitionBy(partition_by)
            write_df.write.mode("append").parquet(path)
        except Exception as e:
            logging.error("Failed to write DataFrame to HDFS at %s: %s", path, e)
            raise
