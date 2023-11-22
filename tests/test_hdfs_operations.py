import pytest
from hdfs_operations import HDFSOperations
from unittest.mock import MagicMock

def test_write_to_hdfs():
    # Mock the SparkSession and DataFrame
    mock_spark = MagicMock()
    mock_df = MagicMock()

    # Instantiate HDFSOperations with mock paths
    hdfs_ops = HDFSOperations("/mock/raw", "/mock/processed")
    hdfs_ops.spark = mock_spark

    # Call the write_to_hdfs method
    hdfs_ops.write_to_hdfs(mock_df, "raw")

    # Assertions can be made on the calls to the mock objects
    # For example, assert if DataFrame's write method was called
    assert mock_df.write.mode("append").parquet.called
