from py4j.protocol import NULL_TYPE
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    lag,
    when,
    sum as window_sum,
    row_number,
    lead,
    concat_ws,
    to_date,
    count,
    min,
    max,
    first,
    lit,
    year,
    to_timestamp,
    coalesce,
)


class UserEventsFeatures:

    def __init__(self, df):
        self.user_events_df = df
        self.clean_df()

    def clean_df(self):
        self.user_events_df.select(
            "user_id", "event_id", "event_timestamp"
        ).dropDuplicates()

    def generate_features(self):
        session_id, is_session_last_event, page_stay_duration = (
            self.is_session_last_event()
        )
        session_year, session_date = self.get_session_dates()
        return (
            self.user_events_df.withColumn(
                "session_id",
                session_id,
            )
            .withColumn(
                "is_session_last_event",
                is_session_last_event,
            )
            .withColumn(
                "page_stay_duration",
                page_stay_duration,
            )
            .withColumn(
                "session_year",
                session_year,
            )
            .withColumn(
                "session_date",
                session_date,
            )
        )

    @staticmethod
    def is_session_last_event(session_expiration_minutes: int = 30):
        """
        Determines if an event is the last event in its session

        Args:
            session_expiration_minutes: Int
                Number of minutes inbetween events before considering a session has ended.
                Default value is 30 minutes

        Dataframe column input:
            event_timestamp: Column
                Timestamp when event occurred.

        Returns:
            Is_session_last_event: Bool
                - True if:
                    - Ordered by event timestamp, previous timestamp event occurred 30 minutes before last event
                    - Last event that occurred per session.
                - False otherwise.


        Logic:
            Uses a conditional statement to check if age is above 40.
        """
        # Define window by user_id and order by event_timestamp
        window_func = Window.partitionBy("user_id").orderBy("event_timestamp")

        # Calculate the time difference (in minutes) between each event and the previous event
        minutes_difference = (
            col("event_timestamp").cast("long")
            - lag("event_timestamp").over(window_func).cast("long")
        ) / 60

        # Define new session start based on time difference > 30 minutes or first event
        is_new_session = when(
            minutes_difference.isNull()
            | (minutes_difference > session_expiration_minutes),
            1,
        ).otherwise(0)

        # Create session_id by using rolling window sum over the session boundaries
        session_id = concat_ws(
            "-",
            to_date("event_timestamp"),
            "user_id",
            window_sum(is_new_session).over(
                window_func.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )

        # Define window to identify the last event per session
        session_window = Window.partitionBy("user_id", session_id).orderBy(
            col("event_timestamp").desc()
        )

        # return column with last event per session
        is_session_last_event = when(
            row_number().over(session_window) == 1,
            True,
        ).otherwise(False)

        # Get Page Stay per page
        page_stay_duration_window = Window.partitionBy(session_id).orderBy(
            "event_timestamp"
        )
        page_stay_duration = coalesce(
            lead(col("event_timestamp"), 1)
            .over(page_stay_duration_window)
            .cast("double")
            - col("event_timestamp").cast("double"),
            lit(0).cast("double"),
        )

        return session_id, is_session_last_event, page_stay_duration

    @staticmethod
    def get_session_dates():
        return year(col("event_timestamp")), to_date("event_timestamp")


class UserEventsFeaturesAggregate:

    def __init__(self, df):
        self.user_events_denormalized_df = df

    def generate_aggregated_df(self):
        user_events_aggregated_df = self.user_events_denormalized_df.groupBy(
            "user_id", "session_year", "session_date", "session_id"
        ).agg(
            min(to_timestamp(col("event_timestamp"))).alias("session_start_at"),
            max(to_timestamp(col("event_timestamp"))).alias("session_end_at"),
            count(col("event_name")).alias("num_events"),
            max(col("device_id")).alias("device_id"),
            self.get_time_spent_in_shopping(),
        )
        return user_events_aggregated_df.select(
            "session_id",
            "user_id",
            "session_end_at",
            "session_start_at",
            "num_events",
            "device_id",
            "time_spent_in_shopping",
            "session_year",
            "session_date",
        )

    @staticmethod
    def get_time_spent_in_shopping():
        return window_sum(
            when(
                col("event_name").contains("shop"), col("page_stay_duration")
            ).otherwise(lit(NULL_TYPE))
        ).alias("time_spent_in_shopping")
