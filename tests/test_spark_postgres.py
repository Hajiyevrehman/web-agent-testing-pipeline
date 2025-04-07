# test_spark_postgres.py
from pyspark.sql import SparkSession
import os
import time

print("--- Testing Spark <-> PostgreSQL Connection ---")

# --- Configuration ---
# !!! UPDATE THIS PATH TO YOUR DOWNLOADED JAR FILE NAME !!!
jdbc_jar_filename = "postgresql-42.7.5.jar" # Or e.g., postgresql-42.6.0.jar etc.
# --- End Configuration ---

jdbc_jar_path = os.path.abspath(f"./{jdbc_jar_filename}") # Assumes JAR is in same directory as script
db_url = "jdbc:postgresql://localhost:5432/web_test_results"
db_properties = {
    "user": "testuser",
    "password": "testpassword",
    "driver": "org.postgresql.Driver"
}
test_table = "spark_connection_test"

if not os.path.exists(jdbc_jar_path):
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f"ERROR: JDBC Jar not found at expected path: {jdbc_jar_path}")
    print(f"Please download the PostgreSQL JDBC driver JAR from:")
    print(f"  https://jdbc.postgresql.org/download/")
    print(f"and place it in the current directory as '{jdbc_jar_filename}'")
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    exit(1)
else:
     print(f"Found JDBC Driver: {jdbc_jar_path}")


print("Initializing SparkSession with JDBC driver...")
spark = None
try:
    spark = SparkSession.builder \
        .appName("SparkPostgresConnectionTest") \
        .master("local[*]") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()

    print("SparkSession created. Attempting to read from Postgres...")

    # Read the test table
    read_df = spark.read.jdbc(url=db_url, table=test_table, properties=db_properties)

    print(f"\nSuccessfully read {read_df.count()} rows from table '{test_table}':")
    read_df.show(truncate=False)

    # Try writing back
    print("\nAttempting to write a new row to Postgres...")
    write_message = f"Spark write test successful at {time.time()}!"
    write_df = spark.createDataFrame([(write_message,)], ["message"]) # Column name must match table

    write_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", test_table) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("append") \
        .save()

    print("Write operation successful!")
    print("----> Please verify the new row exists in the 'spark_connection_test' table using DBeaver/pgAdmin.")

    print("\n--- Spark-Postgres connection test COMPLETE ---")

except Exception as e:
    print(f"\nERROR: Spark-Postgres connection test failed: {e}")
    import traceback
    traceback.print_exc()
finally:
     if spark:
          print("Stopping SparkSession.")
          spark.stop()