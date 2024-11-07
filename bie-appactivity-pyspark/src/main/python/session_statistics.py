import os
import shutil
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils import is_valid_date
from schema import user_events_schema, user_events_table_name
from user_events_features import UserEventsFeatures, UserEventsFeaturesAggregate

if __name__ == "__main__":

    table_location = "/Users/judedinoso/Documents/GitHub/dsf-interview-bie/bie-appactivity-pyspark/src/main/python/spark-warehouse/user_events_aggregated"

    # TEMPORARY WORKAROUND
    # Check if the directory exists, and delete if it does
    if os.path.exists(table_location):
        shutil.rmtree(table_location)

    # Get start and end dates
    if len(sys.argv) > 3:
        print(
            "Print valid parameters for session_statistics.py <start_date> <end_date>"
        )
        sys.exit(-1)

    # Set default dates: yesterday for start_date, today for end_date
    # default_start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    # default_end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    default_start_date = "2023-06-01T00:00:00.000Z"
    default_end_date = "2023-06-02T00:00:00.000Z"

    # Get dates from command line arguments if valid, otherwise use defaults
    start_date = (
        sys.argv[1]
        if len(sys.argv) >= 2 and is_valid_date(sys.argv[1])
        else default_start_date
    )
    end_date = (
        sys.argv[2]
        if len(sys.argv) >= 3 and is_valid_date(sys.argv[2])
        else default_end_date
    )

    # Create Spark Session
    spark = (
        SparkSession.builder.appName("Session Statistics ETL Profile")
        .config("spark.driver.bindAddress", "localhost")
        .getOrCreate()
    )

    # Create table if it does not exist
    if not spark.catalog.tableExists(user_events_table_name):
        user_events_aggregated_df_table = (
            spark.createDataFrame(spark.sparkContext.emptyRDD(), user_events_schema)
            .write.partitionBy("session_date")
            .saveAsTable(user_events_table_name)
        )

    # Read Data from Parquet File
    parquet_file_path = "../../test/resources/app_events/file.parquet"  # Specify the path to your Parquet file
    user_events_raw_df = spark.read.parquet(parquet_file_path).filter(
        (col("event_timestamp") >= start_date) & (col("event_timestamp") < end_date)
    )

    # Create UserEventFeature class to create denormalized dataframe with feature columns
    user_events_feature_generator = UserEventsFeatures(user_events_raw_df)
    user_events_denormalized_df = user_events_feature_generator.generate_features()
    user_events_denormalized_df.orderBy(
        user_events_denormalized_df.user_id,
        user_events_denormalized_df.session_id,
        user_events_denormalized_df.event_timestamp,
    )

    # Create UserEventsFeaturesAggregate class to create aggregated dataframe with feature columns
    user_events_aggregator = UserEventsFeaturesAggregate(user_events_denormalized_df)
    user_events_aggregated_df_staging = user_events_aggregator.generate_aggregated_df()

    # Write Aggregated User Events table
    user_events_aggregated_df_staging.write.insertInto(user_events_table_name)

    user_events_denormalized_df.write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet("sample_denormalized")
    user_events_aggregated_df_staging.write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet("sample_aggregated")
