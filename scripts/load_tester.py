# The "log spammer" script

# scripts/load_tester.py
import argparse
import requests
import time
import datetime
import random
import uuid
import threading
from queue import Queue, Empty


API_KEY='55dc133c-dafc-40de-917b-58bbf54f38b6'
# API_ENDPOINT = "http://localhost/api/ingest/" # Nginx listens on port 80 , but bypass it during development
API_ENDPOINT = "http://localhost:8000/api/ingest/" 
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
SAMPLE_MESSAGES = [
    "User logged in successfully",
    "Processing request",
    "Cache miss for key",
    "Failed to connect to external service",
    "Critical system failure detected",
    "Payment processed",
    "Invalid input received",
]

# --- Shared Counters (Thread-Safe) ---
success_counter = 0
failure_counter = 0
counter_lock = threading.Lock()

def generate_log_entry():
    """Creates a sample log entry dictionary."""
    level = random.choice(LOG_LEVELS)
    message = random.choice(SAMPLE_MESSAGES)
    if level == "ERROR" or level == "CRITICAL":
        message += f" - error code {random.randint(500, 599)}"

    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "level": level,
        "message": f"{message} (ID: {uuid.uuid4()})",
        "metadata": {
            "request_id": str(uuid.uuid4()),
            "user_id": random.randint(1000, 9999),
            "ip_address": f"192.168.1.{random.randint(1, 254)}"
        }
    }

def send_log(session, log_entry):
    """Sends a single log entry to the API (for simple mode)."""
    global success_counter, failure_counter # Use global counters
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
    }
    try:
        response = session.post(API_ENDPOINT, headers=headers, json=log_entry, timeout=5)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        print(f"[{response.status_code}] Log sent successfully: {log_entry['level']} - {log_entry['message'][:30]}...")
        # Use lock for thread safety (though simple mode isn't threaded)
        with counter_lock:
            success_counter += 1
        return True
    except requests.exceptions.RequestException as e:
        print(f"[Error] Failed to send log: {e}")
        with counter_lock:
            failure_counter += 1
        return False
    except Exception as e:
        print(f"[Unexpected Error] {e}")
        with counter_lock:
            failure_counter += 1
        return False

def bulk_worker(log_queue, session):
    """
    Worker function for threads. Pulls logs from a queue and sends them.
    Uses global counters with a lock for thread safety.
    """
    global success_counter, failure_counter # Use global counters
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
    }
    while True: # Keep running until queue is empty
        try:
            log_entry = log_queue.get_nowait()
        except Empty:
            break # No more items in the queue

        try:
            response = session.post(API_ENDPOINT, headers=headers, json=log_entry, timeout=10)
            if 200 <= response.status_code < 300:
                with counter_lock:
                    success_counter += 1
            else:
                # Print error for non-2xx status codes
                print(f"[Error {response.status_code}] Failed: {response.text[:100]}")
                with counter_lock:
                    failure_counter += 1
        except requests.exceptions.RequestException as e:
            print(f"[Request Error] Failed: {e}")
            with counter_lock:
                failure_counter += 1
        except Exception as e:
            print(f"[Unexpected Error] {e}")
            with counter_lock:
                failure_counter += 1
        finally:
            log_queue.task_done() # Signal that this task is complete

def send_bulk_logs(total_logs=1000, concurrency=20):
    """
    Sends a large number of logs concurrently using multiple threads.
    """
    global success_counter, failure_counter # Reset global counters
    success_counter = 0
    failure_counter = 0

    print("\n--- Starting Bulk Load Test ---")
    print(f"Total Logs: {total_logs}")
    print(f"Concurrency Level (threads): {concurrency}")

    log_queue = Queue()
    print("Generating log entries...")
    for _ in range(total_logs):
        log_queue.put(generate_log_entry())
    print(f"{total_logs} log entries generated and queued.")

    threads = []
    start_time = time.time()

    print(f"Starting {concurrency} worker threads...")
    # Use a single session shared by all threads (requests.Session is thread-safe)
    with requests.Session() as session:
        for _ in range(concurrency):
            thread = threading.Thread(target=bulk_worker, args=(log_queue, session), daemon=True)
            threads.append(thread)
            thread.start()

        # Wait for all items in the queue to be processed
        log_queue.join()
        print("All logs processed by workers.")

    # Note: We don't need to explicitly join threads here because
    # log_queue.join() blocks until all tasks are done, meaning threads are finished.

    end_time = time.time()
    duration = end_time - start_time
    logs_per_second = total_logs / duration if duration > 0 else float('inf')

    print("\n--- Bulk Load Test Finished ---")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Logs sent per second: ~{logs_per_second:.2f}") # Approximate rate
    print(f"Successful requests: {success_counter}")
    print(f"Failed requests: {failure_counter}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Ingestion Load Tester")
    parser.add_argument('--mode', type=str, default='simple', choices=['simple', 'bulk'], help='Test mode: "simple" for sequential, "bulk" for concurrent.')
    parser.add_argument('--num_logs', type=int, default=10, help='Number of logs to send (default: 10 for simple, use --num_logs for bulk).')
    parser.add_argument('--concurrency', type=int, default=10, help='Number of concurrent threads for bulk mode (default: 10).')
    args = parser.parse_args()

    # Adjust default num_logs for bulk mode if not specified
    if args.mode == 'bulk' and args.num_logs == 10: # If using default simple num_logs for bulk
         args.num_logs = 1000 # Set a higher default for bulk

    print(f"Starting log tester...")
    print(f"API Endpoint: {API_ENDPOINT}")
    print(f"Using API Key: {API_KEY[:4]}...{API_KEY[-4:]}") # Mask key in output

    if args.mode == 'bulk':
        send_bulk_logs(total_logs=args.num_logs, concurrency=args.concurrency)
    else: # 'simple' mode
        print(f"Sending {args.num_logs} logs sequentially...")
        # Reset counters for simple mode too
        success_counter = 0
        failure_counter = 0
        with requests.Session() as session:
             # Use the global counter increment within send_log
            for _ in range(args.num_logs):
                send_log(session, generate_log_entry())
                time.sleep(0.1) # Add a small delay for simple mode if desired
        print(f"\nFinished. Successfully sent {success_counter}/{args.num_logs} logs.")


# to bulk test use the following command: # python load_tester.py --mode bulk --num_logs 2000 --concurrency 50        