# test_spark.py
from pyspark.sql import SparkSession
import sys

print(f"--- Testing Basic Spark Initialization ---")
print(f"Using Python: {sys.executable}")

try:
    spark = SparkSession.builder \
        .appName("BasicSparkTest") \
        .master("local[*]") \
        .getOrCreate()

    print(f"SparkSession created successfully. Spark version: {spark.version}")
    print(f"Spark Context Web UI: {spark.sparkContext.uiWebUrl}") # Usually http://<hostname>:4040

    df = spark.sql("SELECT 'Basic Spark Test Successful!' as result")
    print("Executing test query...")
    df.show(truncate=False)

    print("--- Basic Spark Test Complete ---")
    spark.stop()

except Exception as e:
    print(f"ERROR: Basic Spark test failed: {e}")
    import traceback
    traceback.print_exc()