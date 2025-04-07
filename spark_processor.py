# spark_processor.py (UPSERT logic added for test_runs table)
import os
import argparse
from urllib.parse import urlparse # For parsing DB URL
import psycopg2 #  <<< ADD THIS IMPORT (Install with: pip install psycopg2-binary)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    ArrayType
)

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
KAFKA_LOG_TOPIC = "website_log_stream"
KAFKA_STATUS_TOPIC = "website_run_status"

# --- DB Config (Only relevant if mode='db') ---
DB_URL = "jdbc:postgresql://localhost:5432/web_test_results"
DB_PROPERTIES = {
    "user": "testuser",
    "password": "testpassword",
    "driver": "org.postgresql.Driver"
}
LOG_TABLE = "agent_logs"
STATUS_TABLE = "test_runs"
# --- End DB Config ---

CHECKPOINT_BASE_DIR = "./spark_checkpoints"

# --- Define Schemas (Keep timestamp fields as StringType) ---
log_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("log_level", StringType(), True),
    StructField("logger_name", StringType(), True),
    StructField("module", StringType(), True),
    StructField("function", StringType(), True),
    StructField("line", IntegerType(), True),
    StructField("message", StringType(), True),
    StructField("exception", ArrayType(StringType(), True), True)
])

status_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("event_type", StringType(), True), # Added back event_type
    StructField("task", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("final_result", StringType(), True)
])

# --- Helper Function for standard DB Append Write (Used for agent_logs) ---
def write_batch_to_postgres(batch_df, batch_id, table_name, db_url, db_properties):
    """Writes a micro-batch DataFrame to PostgreSQL using JDBC APPEND mode."""
    print(f"Processing APPEND batch {batch_id} for table {table_name}...")
    # Cast run_id to string for potential UUID handling in PG
    if "run_id" in batch_df.columns:
        batch_df = batch_df.withColumn("run_id", F.col("run_id").cast(StringType()))

    # Ensure timestamp columns are strings before JDBC write
    # (Dataframe already has them as strings from Kafka source processing)
    for col_name in ["timestamp", "start_time", "end_time"]:
        if col_name in batch_df.columns:
            batch_df = batch_df.withColumn(col_name, F.col(col_name).cast(StringType()))

    try:
        (batch_df.write.format("jdbc")
         .option("url", db_url)
         .option("dbtable", table_name)
         .option("user", db_properties["user"])
         .option("password", db_properties["password"])
         .option("driver", db_properties["driver"])
         .option("stringtype", "unspecified") # For UUID compatibility
         .mode("append") # Simple append for agent_logs
         .save())
        print(f"Successfully wrote APPEND batch {batch_id} to table {table_name}")
    except Exception as e:
        print(f"ERROR writing APPEND batch {batch_id} to {table_name}: {e}")
        # Raise the exception to potentially fail the streaming query
        # Spark's checkpointing will handle retries/state
        raise e

# --- NEW: UPSERT logic functions for test_runs table ---
def process_status_partition(partition_iterator):
    """
    Processes a partition of the status DataFrame to UPSERT into PostgreSQL.
    Establishes one connection per partition.
    """
    conn = None
    cursor = None
    # Parse DB connection details from JDBC URL
    # Format: jdbc:postgresql://host:port/database
    try:
        jdbc_uri = DB_URL.replace("jdbc:", "") # Remove "jdbc:" prefix
        parsed_uri = urlparse(jdbc_uri)
        db_name = parsed_uri.path.lstrip('/')
        db_host = parsed_uri.hostname
        db_port = parsed_uri.port
        db_user = DB_PROPERTIES["user"]
        db_password = DB_PROPERTIES["password"]

        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()

        # SQL for UPSERT using ON CONFLICT DO UPDATE
        # Assumes run_id is the primary key or has a unique constraint
        upsert_sql = f"""
            INSERT INTO {STATUS_TABLE} (run_id, task_description, start_time, end_time, status, final_result)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                task_description = EXCLUDED.task_description,
                -- Only update start_time if the existing one is NULL
                start_time = COALESCE({STATUS_TABLE}.start_time, EXCLUDED.start_time),
                end_time = EXCLUDED.end_time,
                status = EXCLUDED.status,
                final_result = EXCLUDED.final_result;
        """

        for row in partition_iterator:
            # Ensure values passed match the SQL placeholders and DataFrame columns
            # Handle potential None values gracefully for non-string columns if schema changes
            cursor.execute(upsert_sql, (
                row["run_id"],
                row["task_description"],
                row["start_time"], # String format from DataFrame
                row["end_time"],   # String format from DataFrame (or None)
                row["status"],
                row["final_result"]
            ))

        conn.commit() # Commit transaction for the partition
    except Exception as e:
        print(f"ERROR processing/writing status partition: {e}")
        if conn:
            conn.rollback() # Rollback on error within the partition
        # Re-raise the exception to ensure Spark is aware of the failure
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def upsert_status_batch(batch_df, batch_id):
    """
    Applies UPSERT logic for the status table micro-batch using foreachPartition.
    """
    print(f"Processing UPSERT batch {batch_id} for table {STATUS_TABLE}...")
    try:
        # Cast run_id to string just before processing partitions, if needed for UUID
        if "run_id" in batch_df.columns:
             batch_df = batch_df.withColumn("run_id", F.col("run_id").cast(StringType()))

        # Ensure timestamp columns are also cast to string if needed before write
        # (Though they should already be strings based on our select)
        if "start_time" in batch_df.columns:
             batch_df = batch_df.withColumn("start_time", F.col("start_time").cast(StringType()))
        if "end_time" in batch_df.columns:
             batch_df = batch_df.withColumn("end_time", F.col("end_time").cast(StringType()))

        # Process data partition by partition
        batch_df.rdd.foreachPartition(process_status_partition)

        print(f"Successfully processed UPSERT batch {batch_id} for table {STATUS_TABLE}")
    except Exception as e:
        print(f"ERROR during UPSERT batch {batch_id} for {STATUS_TABLE}: {e}")
        # Re-raise to make Spark aware of the failure for checkpointing/retries
        raise e
# --- END NEW UPSERT functions ---


# --- Main Spark Processing Logic ---
def main(output_mode):
    """Main function to run Spark Streaming based on output mode."""
    print(f"Starting Spark Streaming Processor (Output Mode: {output_mode})...")

    log_checkpoint_loc = os.path.join(CHECKPOINT_BASE_DIR, f"logs_{output_mode}")
    status_checkpoint_loc = os.path.join(CHECKPOINT_BASE_DIR, f"status_{output_mode}")
    print(f"Using checkpoint locations: {log_checkpoint_loc}, {status_checkpoint_loc}")

    spark_builder = SparkSession.builder \
        .appName(f"WebsiteAgentLogProcessor-{output_mode}") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5") # Alternate way to include driver

    # JDBC JAR configuration message
    if output_mode == "console":
        print("Console mode: JDBC JAR not required.")
    elif output_mode == "db":
         print("DB mode: Including postgresql driver via spark.jars.packages.")
         # Note: If using spark-submit --jars, remove the .config above

    spark = None
    try:
        spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(f"SparkSession created. Version: {spark.version}")
        print("Reading from Kafka...")

        # --- Read Streams (Added failOnDataLoss option) ---
        log_stream_df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_LOG_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        status_stream_df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_STATUS_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # --- Parse JSON and Select Columns (Keep Timestamps as String) ---
        # Log Stream
        parsed_log_df = log_stream_df_raw \
            .select(F.col("value").cast("string").alias("json_value")) \
            .select(F.from_json(F.col("json_value"), log_schema).alias("data")) \
            .select("data.*")

        # Select columns needed for agent_logs table
        output_log_df = parsed_log_df \
            .withColumn("exception_text", F.concat_ws("\n", F.col("exception"))) \
            .select(
                F.col("run_id"),
                F.col("timestamp"),
                F.col("log_level"),
                F.col("logger_name"),
                F.col("module"),
                F.col("function"),
                F.col("line").cast(IntegerType()),
                F.col("message"),
                F.col("exception_text").alias("exception") # Renamed for clarity
            )

        # Status Stream
        parsed_status_df = status_stream_df_raw \
            .select(F.col("value").cast("string").alias("json_value")) \
            .select(F.from_json(F.col("json_value"), status_schema).alias("data")) \
            .select("data.*")

        # Select columns needed for test_runs table UPSERT
        output_status_df = parsed_status_df \
            .select(
                F.col("run_id"),
                F.col("task").alias("task_description"), # Alias to match example SQL
                F.col("start_time"),
                F.col("end_time"),
                F.col("status"),
                F.col("final_result")
            )

        # --- Write Streams (Conditional Logic) ---
        if output_mode == "console":
            print("Configuring CONSOLE output sinks...")

            log_query = (
                output_log_df.writeStream
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .option("checkpointLocation", log_checkpoint_loc)
                    .trigger(processingTime='15 seconds')
                    .start()
            )

            status_query = (
                output_status_df.writeStream
                    .outputMode("append") # Append mode is okay for console view
                    .format("console")
                    .option("truncate", "false")
                    .option("checkpointLocation", status_checkpoint_loc)
                    .trigger(processingTime='15 seconds')
                    .start()
            )

            print("Streaming queries started writing to CONSOLE.")

        elif output_mode == "db":
            print("Configuring DATABASE output sinks...")

            # --- Log Stream (Uses standard APPEND via JDBC writer) ---
            log_query = (
                output_log_df.writeStream
                    .foreachBatch(lambda df, epoch_id: write_batch_to_postgres(
                        df, epoch_id, LOG_TABLE, DB_URL, DB_PROPERTIES))
                    .option("checkpointLocation", log_checkpoint_loc)
                    .trigger(processingTime='30 seconds') # Adjust trigger as needed
                    .start()
            )

            # --- Status Stream (Uses custom UPSERT logic) ---
            status_query = (
                output_status_df.writeStream
                    .outputMode("update") # Necessary for foreachBatch non-append logic
                    .foreachBatch(upsert_status_batch) # <<< USE NEW UPSERT FUNCTION
                    .option("checkpointLocation", status_checkpoint_loc)
                    .trigger(processingTime='30 seconds') # Adjust trigger as needed
                    .start()
            )

            print("Streaming queries started writing to DATABASE.")

        # --- Await Termination ---
        print("Awaiting termination from streaming queries (press Ctrl+C to stop)...")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"\nERROR during Spark processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
         if spark:
              print("Stopping SparkSession.")
              spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Streaming job to process website agent logs from Kafka.")
    parser.add_argument(
        "--mode",
        choices=['db', 'console'],
        default='db',
        help="Output mode: 'db' (write to PostgreSQL) or 'console'."
    )
    args = parser.parse_args()

    # Ensure checkpoint dirs exist (use correct mode)
    log_cp_path = os.path.join(CHECKPOINT_BASE_DIR, f"logs_{args.mode}")
    status_cp_path = os.path.join(CHECKPOINT_BASE_DIR, f"status_{args.mode}")
    os.makedirs(log_cp_path, exist_ok=True)
    os.makedirs(status_cp_path, exist_ok=True)
    print(f"Ensured checkpoint directories exist: {log_cp_path}, {status_cp_path}")


    main(args.mode)