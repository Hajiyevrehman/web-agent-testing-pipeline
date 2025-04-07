# dataset_viewer.py
import psycopg2
from tabulate import tabulate # Make sure tabulate is installed (pip install tabulate)

# --- Database Connection Details ---
# (Ensure these match your setup)
db_name = "web_test_results"
db_user = "testuser"
db_password = "testpassword"
db_host = "localhost"
db_port = "5432"
# ---

conn = None

try:
    print(f"Connecting to database '{db_name}' on {db_host}:{db_port}...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    print("Connection successful.")

    # --- 1. Display All Test Runs ---
    # Changed title and removed LIMIT 1 from query
    print("\n--- All Test Runs (Sorted by Most Recent First) ---")
    cursor.execute("""
        SELECT run_id, status, task_description, start_time, end_time
        FROM test_runs
        ORDER BY start_time DESC
    """) # Removed LIMIT 1 to get all runs

    # Changed from fetchone() to fetchall()
    all_run_rows = cursor.fetchall() # Get all run rows

    if all_run_rows:
        run_headers = [desc[0] for desc in cursor.description]
        # Pass the list of all rows directly to tabulate
        print(tabulate(all_run_rows, headers=run_headers, tablefmt="grid"))
    else:
        print("No test runs found in the database.")

    # --- Section for displaying logs removed ---
    # Displaying logs for *all* runs could be too much data.
    # If you need logs for a specific run, you might want to query it separately
    # or modify this script to accept a run_id as an argument.

    cursor.close()

except Exception as e:
    print(f"\nERROR: Database connection or query failed: {e}")

finally:
    if conn:
        conn.close()
    print("\nConnection closed.")