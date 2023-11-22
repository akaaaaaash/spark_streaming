from pyspark.sql.functions import col, trim, length, regexp_replace, when, isnan, isnull, to_date
from pyspark.sql.types import IntegerType, StringType, DoubleType
import logging

class DataValidator:
    @staticmethod
    def apply_data_validation(df):
        try:
            # Ensure 'id' is not null and is an integer
            validated_df = df.filter(col("id").isNotNull()).withColumn("id", col("id").cast(IntegerType()))
        except Exception as e:
            logging.error("Validation error for 'id': %s", e)
            raise

        try:
            # Ensure 'name' is a non-empty string and trim it
            validated_df = validated_df.filter(col("name") != "").withColumn("name", trim(col("name")))
        except Exception as e:
            logging.error("Validation error for 'name': %s", e)
            raise

        try:
            # Ensure 'value' is trimmed
            validated_df = validated_df.withColumn("value", trim(col("value")))
        except Exception as e:
            logging.error("Validation error for 'value': %s", e)
            raise

        try:
            # Additional validations
            validated_df = validated_df.filter(length(col("name")) >= 3)
            validated_df = validated_df.withColumn("name", regexp_replace(col("name"), "\\d", ""))
            validated_df = validated_df.withColumn("additional_field",
                                                   when(isnan(col("additional_field")) | isnull(col("additional_field")), "DefaultValue").otherwise(col("additional_field")))
            validated_df = validated_df.withColumn("additional_field", col("additional_field").cast(DoubleType()))
            validated_df = validated_df.filter(col("additional_field") >= 0)
            validated_df = validated_df.withColumn("date_field", to_date(col("date_field"), "yyyy-MM-dd"))
        except Exception as e:
            logging.error("Error in additional validations: %s", e)
            raise

        return validated_df
