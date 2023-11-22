import pytest
from kafka_streaming import KafkaStream
from unittest.mock import MagicMock

def test_consume_data():
    # Mock SparkSession and DataFrame
    mock_spark = MagicMock()
    KafkaStream.spark = mock_spark

    # Instantiate KafkaStream with mock parameters
    kafka_stream = KafkaStream("mock_servers", "mock_topic", "/mock/checkpoint")

    # Call consume_data method
    kafka_stream.consume_data()

    # Assertions can be made on the calls to the mock SparkSession
    # For example, assert if SparkSession's readStream method was called
    assert mock_spark.readStream.format("kafka").load.called
