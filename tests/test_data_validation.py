import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from data_validation import DataValidator

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("DataValidationTests").getOrCreate()

def test_apply_data_validation(spark):
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", StringType()),
        StructField("additional_field", StringType()),  # Simulating a string that should be a double
        StructField("date_field", StringType())  # Simulating a string date field
    ])
    data = [
        (1, "   Name 1   ", "Value1", "100", "2021-01-01"),
        (None, "Name2", "Value2", "NaN", "2021-01-32"),  # Invalid date
        (2, "Name3", "Value3", None, "2021-01-02"),
        (-3, "Name4", "Value4", "-50", "InvalidDate"),  # Negative id, additional_field, and invalid date
        (4, "123Name5", " Value5 ", "50.5", "2021-01-03")  # Name with numbers
    ]
    df = spark.createDataFrame(data, schema)

    # Apply data validation
    validated_df = DataValidator.apply_data_validation(df)

    # Collect results and perform assertions
    results = validated_df.collect()

    # Assert the count after filtering
    assert len(results) == 2

    # Assert specific validations
    assert all(row.id > 0 for row in results)  # ID is positive
    assert all(row.name.isalpha() for row in results)  # Name has only alphabets
    assert all(" " not in row.name for row in results)  # Name is trimmed
    assert all(isinstance(row.additional_field, float) and row.additional_field >= 0 for row in results)  # Additional field is non-negative float
    assert all(row.date_field is not None for row in results)  # Date field is valid
