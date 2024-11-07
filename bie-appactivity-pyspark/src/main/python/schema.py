from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ShortType,
    DoubleType,
)

user_events_table_name = "user_events_aggregated"
user_events_schema = StructType(
    [
        StructField("session_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("session_end_at", TimestampType(), nullable=True),
        StructField("session_start_at", TimestampType(), nullable=True),
        StructField("num_events", ShortType(), nullable=True),
        StructField("device_id", StringType(), nullable=True),
        StructField("time_spent_in_shopping", DoubleType(), nullable=True),
        StructField("session_year", ShortType(), nullable=False),
    ]
)
