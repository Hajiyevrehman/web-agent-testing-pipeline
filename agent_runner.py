# agent_runner.py (Modularized with command-line arguments)
import sys
import site
import asyncio
import os
import json
import uuid
import logging
import traceback
import argparse # <<< ADD THIS IMPORT
from datetime import datetime, timezone
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

# Import Agent - doing this early might trigger logging setup
from browser_use import Agent
# from browser_use.state import AgentState # Optional

# Import Kafka related modules
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Load Environment Variables ---
load_dotenv()

# --- Kafka Configuration ---
KAFKA_BROKER = '127.0.0.1:9092' # Use localhost to match broker's advertised listener
KAFKA_LOG_TOPIC = 'website_log_stream'
KAFKA_STATUS_TOPIC = 'website_run_status'

# --- Kafka Producer Initialization ---
def create_kafka_producer():
    """Initializes and returns a KafkaProducer instance."""
    try:
        # logging.getLogger('kafka').setLevel(logging.WARNING) # Optional
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id='browser-agent-runner-logger-async',
            linger_ms=100,
            batch_size=16384,
            retries=3,
            request_timeout_ms=60000 # 60 seconds
        )
        print(f"Kafka Producer initialized for broker {KAFKA_BROKER}.")
        return producer
    except Exception as e:
        print(f"ERROR: Failed to initialize Kafka Producer: {e}")
        return None

# --- Custom Logging Handlers (AsyncQueueLogHandler, ListLogHandler) ---
# (Keep these classes as they are)
class AsyncQueueLogHandler(logging.Handler):
    def __init__(self, queue: asyncio.Queue, run_id: str):
        super().__init__()
        self.queue = queue
        self.run_id = run_id
        self.setLevel(logging.INFO)
    def format_record(self, record: logging.LogRecord) -> dict:
        message = self.format(record)
        logger_name = getattr(record, 'name_simple', record.name)
        log_entry = {
            'run_id': self.run_id, 'event_type': 'AGENT_LOG',
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'log_level': record.levelname, 'logger_name': logger_name,
            'module': record.module, 'function': record.funcName, 'line': record.lineno,
            'message': message,
        }
        if record.exc_info:
            log_entry['exception'] = traceback.format_exception(*record.exc_info)
        return log_entry
    def emit(self, record: logging.LogRecord):
        if record.levelno < self.level: return
        try:
            if isinstance(record.name, str) and record.name.startswith('browser_use.'):
                 record.name_simple = record.name.split('.')[-2]
            else:
                 record.name_simple = record.name
            log_entry = self.format_record(record)
            self.queue.put_nowait(log_entry)
        except asyncio.QueueFull:
            print(f"WARN ({self.__class__.__name__}): Log queue is full. Log message dropped.", file=sys.stderr)
        except Exception as e:
            self.handleError(record)
            print(f"ERROR ({self.__class__.__name__}) formatting/queueing log record: {e}", file=sys.stderr)

class ListLogHandler(logging.Handler):
    def __init__(self, log_list: list):
        super().__init__()
        self.log_list = log_list
        self.setLevel(logging.INFO)
    def emit(self, record: logging.LogRecord):
        if record.levelno < self.level: return
        try:
            if isinstance(record.name, str) and record.name.startswith('browser_use.'):
                 record.name_simple = record.name.split('.')[-2]
            else:
                 record.name_simple = record.name
            self.log_list.append(self.format(record))
        except Exception as e:
            self.handleError(record)
            print(f"ERROR ({self.__class__.__name__}) appending log record: {e}", file=sys.stderr)
# --- End Custom Logging Handlers ---


# --- Kafka Log Sending Task ---
# (Keep this function as it is)
async def kafka_log_sender(queue: asyncio.Queue, producer: KafkaProducer, topic: str):
    print("Starting Kafka log sender task...")
    while True:
        log_entry = None
        try:
            log_entry = await queue.get()
            if log_entry is None:
                print("Kafka log sender received shutdown signal.")
                queue.task_done()
                break
            if producer:
                try:
                    producer.send(topic, value=log_entry)
                except NoBrokersAvailable:
                     print(f"ERROR (log sender task): Kafka broker not available at {KAFKA_BROKER}.")
                except Exception as e_send:
                    print(f"ERROR (log sender task): Failed to send message to Kafka: {e_send}")
                    print(f"Failed message data: {log_entry}")
            else:
                print("WARN (log sender task): Kafka producer is None. Log message dropped.")
            queue.task_done()
        except asyncio.CancelledError:
            print("Kafka log sender task cancelled.")
            break
        except Exception as e_task:
            print(f"ERROR in Kafka log sender task processing item: {e_task}")
            if log_entry: queue.task_done()
            await asyncio.sleep(1)
    print("Kafka log sender task finished.")

# --- Helper function to send final status ---
# (Keep this function as it is)
def send_status_update(producer, topic, run_data):
    if not producer:
        print("WARN: Kafka producer is None, cannot send status update.")
        return False
    try:
        if 'event_type' not in run_data:
             run_data['event_type'] = 'STATUS_UPDATE'
        print(f"Attempting to send status update: {run_data.get('event_type')} for Run ID: {run_data.get('run_id')}")
        future = producer.send(topic, value=run_data)
        print(f"Sent {run_data.get('event_type')} status to Kafka topic '{topic}' (Run ID: {run_data.get('run_id')})")
        return True
    except Exception as e:
        print(f"ERROR: Failed to send status update message to Kafka: {e}")
        print(f"Data: {run_data}")
        return False

# --- Agent Simulation Function ---
# (Keep this function as it is)
async def run_agent_task(agent: Agent):
    print("\n--- Running Agent Task... ---")
    agent_final_result_obj = None
    try:
        agent_final_result_obj = await agent.run()
        print("--- Agent Task Run Attempt Complete ---")
        return agent_final_result_obj, None
    except Exception as e:
        logging.exception(f"Agent execution threw an unhandled exception: {e}")
        print(f"\nERROR: Agent execution threw an exception: {e}")
        return None, e


# --- Main Execution Function ---
# <<< MODIFIED: Accepts target_url and task_prompt as arguments >>>
async def main(target_url: str, task_prompt: str):
    run_id = str(uuid.uuid4()) # Generate unique ID first
    start_time_iso = datetime.now(timezone.utc).isoformat() # Record start time

    producer = create_kafka_producer()
    if not producer:
        print("CRITICAL: Kafka Producer could not be initialized. Exiting.")
        return

    # --- Setup Log Handlers and Queue (Unchanged) ---
    log_queue = asyncio.Queue(maxsize=1000)
    log_buffer = []
    kafka_handler = AsyncQueueLogHandler(log_queue, run_id)
    list_handler = ListLogHandler(log_buffer)
    log_formatter = logging.Formatter('%(levelname)-8s [%(name)s] %(message)s')
    kafka_handler.setFormatter(log_formatter)
    list_handler.setFormatter(log_formatter)

    # --- Define Task based on input arguments ---
    # target_site = "https://quotes.toscrape.com/" # <<< REMOVED HARDCODED
    initial_setup_actions = [{'open_tab': {'url': target_url}}] # <<< USE target_url
    # task_description = f"You are on '{target_site}'. Give me 5 top quotes with love tag" # <<< REMOVED HARDCODED

    print(f"\nInitializing Agent for Run ID: {run_id}")
    print(f"Target Site: {target_url}") # <<< USE target_url
    print(f"Task: {task_prompt}") # <<< USE task_prompt

    agent_final_result_obj = None
    agent_exception = None
    final_status_str = 'FAILURE'
    agent = None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not found in .env file.")
        final_status_str = 'FAILURE'
        agent_final_result_obj = "ERROR: OPENAI_API_KEY missing"
        # Send failure status immediately if possible
        failure_data = {
            'run_id': run_id, 'event_type': 'TASK_END', 'task': task_prompt, # <<< USE task_prompt
            'start_time': start_time_iso, 'end_time': datetime.now(timezone.utc).isoformat(),
            'status': 'FAILURE', 'final_result': agent_final_result_obj }
        send_status_update(producer, KAFKA_STATUS_TOPIC, failure_data)
    else:
        try:
            llm = ChatOpenAI(model="gpt-4o", temperature=0.1)
            agent = Agent(
                llm=llm,
                task=task_prompt, # <<< USE task_prompt
                initial_actions=initial_setup_actions,
            )
            # *** Agent init done ***
        except Exception as init_err:
            print(f"ERROR: Failed to initialize LLM or Agent: {init_err}")
            final_status_str = 'FAILURE'
            agent_final_result_obj = f'ERROR: Agent/LLM Init Failed - {init_err}'
            # Send failure status immediately
            failure_data = {
                'run_id': run_id, 'event_type': 'TASK_END', 'task': task_prompt, # <<< USE task_prompt
                'start_time': start_time_iso, 'end_time': datetime.now(timezone.utc).isoformat(),
                'status': 'FAILURE', 'final_result': agent_final_result_obj }
            send_status_update(producer, KAFKA_STATUS_TOPIC, failure_data)

    # --- Configure Logging Handlers AFTER Agent Init ---
    if agent: # Only configure handlers if agent initialized
        # (Logging setup remains the same)
        root_logger = logging.getLogger()
        browser_use_logger = logging.getLogger('browser_use')
        current_root_level = root_logger.getEffectiveLevel()
        if current_root_level > logging.INFO: root_logger.setLevel(logging.INFO)
        current_bu_level = browser_use_logger.getEffectiveLevel()
        if current_bu_level > logging.INFO: browser_use_logger.setLevel(logging.INFO)
        root_logger.addHandler(kafka_handler)
        root_logger.addHandler(list_handler)
        browser_use_logger.addHandler(kafka_handler)
        browser_use_logger.addHandler(list_handler)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_formatter)
        has_console_handler = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers if h is not console_handler)
        if not has_console_handler: root_logger.addHandler(console_handler)

        print(f"Logging configured AFTER agent init. Logs sent via queue to Kafka topic '{KAFKA_LOG_TOPIC}'. Run ID: {run_id}")
        logging.info(f"--- Starting Test Run --- Run ID: {run_id}") # Logged by root

        # --- Send TASK_START Status Update to Kafka ---
        task_start_status_data = {
            'run_id': run_id,
            'event_type': 'TASK_START', # Differentiate from end event
            'task': task_prompt, # <<< USE task_prompt
            'start_time': start_time_iso,
            'end_time': None,
            'status': 'RUNNING',
            'final_result': None
        }
        send_status_update(producer, KAFKA_STATUS_TOPIC, task_start_status_data)
        # --- End TASK_START send ---

        # --- Start Background Sender ---
        sender_task = asyncio.create_task(
            kafka_log_sender(log_queue, producer, KAFKA_LOG_TOPIC)
        )

        # --- Run Simulation ---
        agent_final_result_obj, agent_exception = await run_agent_task(agent)

        # --- Determine Final Status ---
        # (Logic remains the same)
        if agent_exception:
            final_status_str = 'FAILURE'
            agent_final_result_obj = f"Agent failed with exception: {str(agent_exception)}"
        else:
            log_content = "\n".join(log_buffer)
            if "✅ Task completed" in log_content or "✅ Successfully" in log_content:
                 final_status_str = 'SUCCESS'
            else:
                 final_status_str = 'FAILURE'
                 agent_final_result_obj = agent_final_result_obj or "Agent finished run but success indicators not found in logs."

        # --- Send Final Status Message ---
        print(f"\nSimulation attempt finished. Final Status (based on logs): {final_status_str}")
        task_end_status_data = {
             'run_id': run_id,
             'event_type': 'TASK_END',
             'task': task_prompt, # <<< USE task_prompt
             'start_time': start_time_iso,
             'end_time': datetime.now(timezone.utc).isoformat(),
             'status': final_status_str,
             'final_result': str(agent_final_result_obj)
        }
        send_status_update(producer, KAFKA_STATUS_TOPIC, task_end_status_data)

        # --- Shutdown background sender task gracefully ---
        # (Logic remains the same)
        print("Signaling log sender task to shut down...")
        await log_queue.put(None)
        print("Waiting for log queue to empty...")
        await log_queue.join()
        print("Waiting for sender task to finish...")
        await asyncio.sleep(0.5)
        if not sender_task.done():
            print("Cancelling sender task (if not already finished)...")
            sender_task.cancel()
            try: await sender_task
            except asyncio.CancelledError: print("Sender task successfully cancelled.")

        # --- Cleanup Logging Handlers ---
        # (Logic remains the same)
        root_logger.removeHandler(kafka_handler)
        root_logger.removeHandler(list_handler)
        browser_use_logger.removeHandler(kafka_handler)
        browser_use_logger.removeHandler(list_handler)
        if 'console_handler' in locals() and console_handler in root_logger.handlers:
             root_logger.removeHandler(console_handler)

    # --- Cleanup Kafka Producer ---
    # (Logic remains the same)
    if producer:
        print("Flushing Kafka messages (final)...")
        producer.flush(timeout=10)
        print("Closing Kafka producer.")
        producer.close(timeout=10)

    print(f"--- Run Finished --- Run ID: {run_id}")



# --- Script Entry Point ---
if __name__ == "__main__":
    # <<< ADDED Argument Parsing >>>
    parser = argparse.ArgumentParser(description="Run the BrowserUse Agent with Kafka logging.")
    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help="The target URL for the agent to start on."
    )
    parser.add_argument(
        "--prompt",
        type=str,
        required=True,
        help="The task description/prompt for the agent."
    )
    args = parser.parse_args()

    # Print info before starting
    print(f"Sending detailed logs to Kafka topic '{KAFKA_LOG_TOPIC}'.")
    print(f"Sending final status to Kafka topic '{KAFKA_STATUS_TOPIC}'.")
    print(f"Using Broker '{KAFKA_BROKER}'. Make sure Kafka is running.")

    # <<< MODIFIED: Pass arguments to main >>>
    asyncio.run(main(target_url=args.url, task_prompt=args.prompt))