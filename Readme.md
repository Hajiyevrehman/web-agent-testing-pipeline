# LLM Web Agent Pipeline with Kafka, Spark, and PostgreSQL

## Overview

This project demonstrates an end-to-end pipeline for running automated web tasks using an LLM-powered browser agent (`browser-use`). It captures detailed logs and run status in real-time, streams them via Apache Kafka, processes the data using Apache Spark Streaming, and stores the results persistently in a PostgreSQL database for analysis.

## Features

* **LLM-Driven Automation:** Leverages the `browser-use` library and an OpenAI model (GPT-4o) to perform tasks described in natural language on target websites.
* **Real-time Event Streaming:** Uses Kafka to decouple the agent runner from data processing, handling logs and status updates as separate streams.
* **Scalable Data Processing:** Employs Spark Streaming to process incoming data, parse JSON messages, and handle database writes efficiently.
* **Persistent Storage:** Stores structured run summaries and detailed agent logs in PostgreSQL.
* **Modular Execution:** Allows running multiple agent instances concurrently with different tasks and target URLs via command-line arguments.
* **Data Querying:** Includes a simple script to view processed run data directly from the database.

## Architecture (Conceptual)

+-----------------+      +---------------------+      +--------------------+      +--------------------+
| Agent Runner(s) |----->| Kafka               |----->| Spark Processor    |----->| PostgreSQL         |
| (agent_runner.py|      | (Logs & Status Topics|      | (spark_processor.py|      | (test_runs,       |
|  w/ browser-use)|      |  in Docker)         |      |  reads Kafka)      |      |  agent_logs tables)|
+-----------------+      +---------------------+      +--------------------+      +--------------------+
^
|
+--------------------+
| Data Viewer        |
| (dataset_viewer.py)|
+--------------------+


## Prerequisites

* **Docker & Docker Compose:** To run Kafka, Zookeeper, and PostgreSQL services. Install from [Docker's official website](https://docs.docker.com/get-docker/).
* **Python 3.11:** The scripts are developed using Python 3.11. Ensure it's installed and available in your PATH.
* **`pip`:** Python package installer (usually comes with Python).
* **`spark-submit`:** Command-line tool for submitting Spark jobs. Needs a local Spark installation or environment setup where `spark-submit` is available.
* **OpenAI API Key:** You need an API key from OpenAI to use the LLM.

## Setup Instructions

1.  **Clone Project Repository:**
    ```bash
    git clone [https://github.com/Hajiyevrehman/web-agent-testing-pipeline.git](https://github.com/Hajiyevrehman/web-agent-testing-pipeline.git)
    cd web-agent-testing-pipeline
    ```

2.  **Clone `browser-use` Library:** (Assuming it's not a submodule)
    ```bash
    git clone [https://github.com/BrowserUse/browser-use.git](https://github.com/BrowserUse/browser-use.git)
    ```
    *(Place the `browser-use` folder inside the `web-agent-testing-pipeline` directory)*

3.  **Create Python Virtual Environment:**
    ```bash
    python3.11 -m venv venv
    source venv/bin/activate
    ```
    *(On Windows, use `venv\Scripts\activate`)*

4.  **Install Dependencies:**
    ```bash
    pip install --upgrade pip
    # Install agent library in editable mode
    pip install -e ./browser-use
    # Install other Python requirements
    pip install python-dotenv langchain-openai httpx~=0.25 kafka-python pyspark psycopg2-binary tabulate
    ```

5.  **Install Playwright Browser:**
    ```bash
    playwright install chromium
    ```

6.  **Configure OpenAI API Key:**
    * Create a file named `.env` in the project's root directory (`web-agent-testing-pipeline`).
    * Add your OpenAI API key to it:
        ```dotenv
        OPENAI_API_KEY="your_openai_key_here"
        ```

7.  **Start Docker Services:**
    * Make sure Docker Desktop (or Docker daemon) is running.
    * From the project's root directory (`web-agent-testing-pipeline`, where `docker-compose.yml` is located):
        ```bash
        docker-compose up -d
        ```
    * This will start Zookeeper, Kafka, and PostgreSQL containers in the background.

8.  **Database Schema Setup:**
    * **Crucial Step:** Before running the Spark Processor in `db` mode for the first time (or if you want to reset the tables), you need to create the necessary tables in the PostgreSQL database.
    * You can use a database GUI tool like DBeaver, pgAdmin, or the `psql` command-line tool to connect to the database running in Docker.
        * **Connection Details (Defaults from `docker-compose.yml`):**
            * Host: `localhost`
            * Port: `5432`
            * Database: `web_test_results`
            * User: `testuser`
            * Password: `testpassword`
    * Execute the following SQL script in the `web_test_results` database:

        ```sql
        -- Drop tables first if they already exist, to ensure a clean setup
        DROP TABLE IF EXISTS agent_logs;
        DROP TABLE IF EXISTS test_runs;

        -- Table to store overall test run summary information
        -- Data primarily comes from the 'website_run_status' Kafka topic
        CREATE TABLE test_runs (
            run_id UUID PRIMARY KEY,                      -- Unique identifier for the run
            task_description TEXT,                        -- The task given to the agent
            start_time TIMESTAMPTZ NOT NULL,              -- When the run started (Timestamp with Time Zone)
            end_time TIMESTAMPTZ,                         -- When the run finished (or was last updated) - ALLOW NULL initially
            status VARCHAR(10) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILURE', 'UNKNOWN')), -- Final (or current) outcome
            final_result TEXT,                            -- Final message/result/error from the agent (as string)
            ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- When this record was first inserted/last updated by Spark
        );

        -- Add indexes for faster querying on common filters
        CREATE INDEX idx_test_runs_status ON test_runs(status);
        CREATE INDEX idx_test_runs_time ON test_runs(start_time, end_time);

        -- Table to store detailed agent log events
        -- Data comes from the 'website_log_stream' Kafka topic
        CREATE TABLE agent_logs (
            log_id BIGSERIAL PRIMARY KEY,                 -- Auto-incrementing unique ID for each log entry
            run_id UUID NOT NULL REFERENCES test_runs(run_id) ON DELETE CASCADE, -- Link to the specific test run
            timestamp TIMESTAMPTZ NOT NULL,               -- Original timestamp of the log event from the agent
            log_level VARCHAR(10),                        -- e.g., INFO, WARNING, ERROR
            logger_name VARCHAR(150),                     -- Name of the logger (e.g., agent, controller, root)
            module VARCHAR(100),                          -- Python module where log occurred
            "function" VARCHAR(100),                      -- Function name where log occurred (quoted because function is SQL keyword)
            line INTEGER,                                 -- Line number in the module
            message TEXT,                                 -- The actual formatted log message
            exception TEXT NULL,                          -- Formatted exception traceback, if any (allows NULL)
            ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- When this record was inserted by Spark
        );

        -- Add indexes for faster querying on logs
        CREATE INDEX idx_agent_logs_run_id ON agent_logs(run_id);
        CREATE INDEX idx_agent_logs_timestamp ON agent_logs("timestamp"); -- Quote timestamp as it's a keyword
        CREATE INDEX idx_agent_logs_level ON agent_logs(log_level);
        CREATE INDEX idx_agent_logs_logger ON agent_logs(logger_name);
        ```
    * **Note:** This script includes `DROP TABLE` commands. Running it will **delete any existing data** in the `agent_logs` and `test_runs` tables before recreating them with this structure. It also defines specific constraints and indexes for data integrity and performance.

## Running the Pipeline

1.  **Ensure Docker Services are Running:**
    ```bash
    docker-compose ps
    ```
    *(Make sure `kafka`, `zookeeper`, and `postgres_db` containers are 'Up')*
    If not, run `docker-compose up -d`.

2.  **Start the Spark Processor:**
    * Activate the virtual environment if not already active: `source venv/bin/activate`
    * Run the `spark-submit` command. Using `--packages` is recommended for the PostgreSQL driver:
        ```bash
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.5 \
          spark_processor.py \
          --mode db
        ```
    * This job will run continuously, listening for messages on the Kafka topics and writing them to PostgreSQL. Keep this terminal window open or run it in the background (e.g., using `nohup` or `screen`/`tmux`).
    * *(Optional - Console Mode):* To see Spark output directly in the console instead of writing to DB (useful for debugging), change the last line to `--mode console`.

3.  **Run the Agent Runner:**
    * Open a **new** terminal window.
    * Activate the virtual environment: `source venv/bin/activate`
    * Run the agent with your desired URL and prompt:
        ```bash
        python agent_runner.py --url "[https://www.example.com](https://www.example.com)" --prompt "Find the contact email address"
        ```
    * Replace `"https://www.example.com"` and `"Find the contact email address"` with your target URL and task.
    * You can launch **multiple instances** of `agent_runner.py` simultaneously in different terminals, each with potentially different URLs and prompts. Each run will generate logs and status updates sent to Kafka and processed by Spark.

## Viewing Results

1.  **Using `dataset_viewer.py`:**
    * Make sure the virtual environment is active: `source venv/bin/activate`
    * *(Optional)*: If you changed the default database connection details in `docker-compose.yml`, update the connection variables at the top of `dataset_viewer.py`.
    * Run the script:
        ```bash
        python dataset_viewer.py
        ```
    * This will display the most recent test run summary and its associated logs fetched from the PostgreSQL database.

2.  **Using a Database GUI (Recommended for exploration):**
    * Connect to the PostgreSQL database using a tool like DBeaver, pgAdmin, or DataGrip.
    * Use the connection details mentioned in the "Database Schema Setup" section.
    * You can then browse the `test_runs` and `agent_logs` tables, run custom SQL queries, sort data, etc.

## Configuration

* **OpenAI API Key:** Must be set in the `.env` file.
* **Kafka Broker:** The address (`127.0.0.1:9092`) is currently hardcoded in `agent_runner.py` and `spark_processor.py`. Modify if your Kafka setup differs.
* **Kafka Topics:** Topic names (`website_log_stream`, `website_run_status`) are defined in the Python scripts.
* **Database Connection:** Credentials in `.env` (for potential future use), `docker-compose.yml` (for the service itself), `spark_processor.py` (DB properties), and `dataset_viewer.py`. Ensure consistency if defaults are changed.

## Troubleshooting

* **Spark Checkpoint Issues:** If the Spark job fails and you need to restart it cleanly after fixing an issue, you might need to remove the old checkpoint directories:
    ```bash
    rm -rf ./spark_checkpoints/logs_db
    rm -rf ./spark_checkpoints/status_db
    # Or use _console if you were running in console mode
    # rm -rf ./spark_checkpoints/logs_console
    # rm -rf ./spark_checkpoints/status_console
    ```
* **Kafka Connection Errors:** Ensure the Kafka container is running (`docker-compose ps`) and that the broker address used in the Python scripts matches the `KAFKA_ADVERTISED_LISTENERS` setting in `docker-compose.yml` (specifically the `PLAINTEXT://` part accessible from your host).
* **Database Connection Errors:** Verify the PostgreSQL container is running, and the connection details (host, port, dbname, user, password) are correct in the scripts (`spark_processor.py`, `dataset_viewer.py`) or GUI tool.
* **Dependency Issues:** Double-check that all `pip install` commands completed successfully within the activated virtual environment (`venv`).

*(Future Work Section - Optional)*
* *Add data visualization dashboards (e.g., using Tableau, Power BI, Grafana).*
* *Implement error alerting based on Spark processing failures or high agent failure rates.*
* *Add more sophisticated data transformations or enrichments in the Spark job.*