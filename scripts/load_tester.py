# The "log spammer" script

# scripts/load_tester.py
import requests
import time
import datetime
import random
import uuid


API_KEY='64e19f35-b52f-4720-806c-110a8246b56a'
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
    """Sends a single log entry to the API."""
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
    }
    try:
        response = session.post(API_ENDPOINT, headers=headers, json=log_entry, timeout=5)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        print(f"[{response.status_code}] Log sent successfully: {log_entry['level']} - {log_entry['message'][:30]}...")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[Error] Failed to send log: {e}")
        return False
    except Exception as e:
        print(f"[Unexpected Error] {e}")
        return False


if __name__ == "__main__":
    num_logs = 10 # Number of logs to send
    delay = 0.5 # Delay between logs in seconds

    print(f"Starting log tester...")
    print(f"API Endpoint: {API_ENDPOINT}")
    print(f"Using API Key: {API_KEY[:4]}...{API_KEY[-4:]}") # Mask key in output
    print(f"Sending {num_logs} logs with a {delay}s delay between each.")

    # Use a session for connection pooling
    with requests.Session() as session:
        success_count = 0
        for i in range(num_logs):
            log = generate_log_entry()
            if send_log(session, log):
                success_count += 1
            time.sleep(delay)

    print(f"\nFinished.")
    print(f"Successfully sent {success_count}/{num_logs} logs.")