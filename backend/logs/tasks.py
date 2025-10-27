import json
import logging
import os
import uuid
from datetime import datetime
from celery import shared_task, group
from django.core.cache import caches
from django.db import transaction
from .models import Application, LogEntry

logger = logging.getLogger(__name__)

# -----------Architecture-Flow------------------------------------
# Producer (API) : pushes logs to Redis stream logs:queue.
# Celery Master Task (process_log_master) : reads ~500 logs from Redis and splits them into 5 parallel subtasks of 100 each.
# Celery Subtasks (process_log_chunk) → each subtask handles inserts concurrently using bulk_create.


@shared_task(bind=True)
def process_log_chunk(self, entries, stream_name, group_name, message_ids):
    """Subtask that processes a batch of decoded log entries."""
    redis_client = caches["log_queue"].client.get_client()
    dlq_stream_name = "logs:dlq"
    dlq_max_len = 10000  # Cap the DLQ to prevent unbounded memory growth

    try:
        app_ids = {int(e["application_id"]) for e in entries}
        # Fetch all unique applications in a single query
        apps = {a.id: a for a in Application.objects.filter(id__in=app_ids)}

        to_create = []
        failed_entries = [] # For entries that fail validation

        for entry_data, msg_id in zip(entries, message_ids):
            try:
                # Defensive validation for each field
                if "application_id" not in entry_data:
                    raise KeyError("Missing 'application_id' field.")
                app_id = int(entry_data["application_id"])
                app = apps.get(app_id)
                if not app:
                    raise Application.DoesNotExist(f"Application with ID {app_id} not found.")

                if "timestamp" not in entry_data:
                    raise KeyError("Missing 'timestamp' field.")
                timestamp = datetime.fromisoformat(entry_data["timestamp"])

                metadata = json.loads(entry_data.get("metadata", "{}"))

                to_create.append(
                    LogEntry(
                        application=app,
                        timestamp=timestamp,
                        level=entry_data["level"].upper(),
                        message=entry_data["message"],
                        metadata=metadata,
                    )
                )
            except (KeyError, ValueError, Application.DoesNotExist, json.JSONDecodeError) as e:
                logger.warning(f"Failed to process log entry {msg_id}. Moving to DLQ. Reason: {e}")
                # Avoid double-encoding: store a flat structure in the DLQ
                dlq_entry = entry_data.copy()
                dlq_entry['error'] = str(e)
                dlq_entry['original_id'] = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id
                dlq_entry['failed_at'] = datetime.utcnow().isoformat()
                failed_entries.append(dlq_entry)

        if to_create:
            with transaction.atomic():
                LogEntry.objects.bulk_create(to_create, batch_size=100)
            logger.info(f"Successfully processed and saved {len(to_create)} logs.")

        # Use a pipeline for atomic and efficient Redis operations
        if message_ids:
            pipe = redis_client.pipeline()
            # Move all failed entries to the Dead Letter Queue
            for item in failed_entries:
                pipe.xadd(dlq_stream_name, item, maxlen=dlq_max_len)
            
            # Acknowledge all messages in this chunk from the original stream
            pipe.xack(stream_name, group_name, *message_ids)
            pipe.execute()

        # Return a structured result for better observability
        return {"processed": len(to_create), "dlq": len(failed_entries)}

    except Exception as exc:
        logger.error(f"Error in process_log_chunk: {exc}")
        # Do not acknowledge messages if processing failed. They will be re-processed later.
        return {"processed": 0, "dlq": 0, "error": str(exc)}

@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def process_log_master(self, batch_size=500):
    """
    queue based log inngestion using redis streams
    """
    redis_client = caches["log_queue"].client.get_client()
    try:
        stream_name = "logs:queue"
        group_name = "log_consumers"
        dlq_stream_name = "logs:dlq"
        dlq_max_len = 10000
        
        # Create a unique consumer name for each worker process to ensure true parallel processing.
        # Combining hostname with PID is robust for multi-process workers on a single machine.
        try:
            worker_hostname = self.request.hostname
            process_id = os.getpid()
            consumer_name = f"{worker_hostname}-{process_id}"
        except Exception as e:
            logger.warning(f"Could not reliably determine hostname/PID: {e}. Using UUID fallback.")
            consumer_name = f"worker-{uuid.uuid4().hex}"

        lock_key = f"lock:create-group:{stream_name}:{group_name}"

        # Use a Redis lock to prevent a race condition on group creation
        # when multiple workers start at the same time.
        with redis_client.lock(lock_key, timeout=10):
            try:
                # Check if group exists by getting group info
                redis_client.xinfo_groups(stream_name)
            except Exception:
                # If xinfo_groups fails, the stream or group likely doesn't exist.
                # It's safe to try and create it.
                logger.info(f"Consumer group '{group_name}' or stream '{stream_name}' not found. Attempting to create.")
                redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                logger.info(f"Successfully created consumer group '{group_name}'.")

        # read upto batchsize entries from queue
        entries = redis_client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"},
            count=batch_size,
            block=2000,
        )

        if not entries:
            logger.debug("No logs in queue to process.")
            return {"status": "idle", "processed": 0}
        
        # Group messages and their IDs together for chunking
        entries_with_ids = []

        # flatten queue responses
        for _, msgs in entries:
            for msg_id, data in msgs:
                try:
                    decoded_entry = {
                            "application_id": data[b"application_id"].decode(),
                            "timestamp": data[b"timestamp"].decode(),
                            "level": data[b"level"].decode(),
                            "message": data[b"message"].decode(),
                            "metadata": data[b"metadata"].decode(),
                        }
                    entries_with_ids.append({'id': msg_id, 'data': decoded_entry})
                except Exception as e:
                    logger.warning(f"Malformed message {msg_id} skipped. Moving to DLQ. Error: {e}")
                    # Move malformed message to DLQ, avoiding double-encoding
                    dlq_payload = {k.decode(): v.decode() for k, v in data.items()}
                    dlq_payload['error'] = f"DECODE_ERROR: {e}"
                    dlq_payload['original_id'] = msg_id.decode()
                    dlq_payload['failed_at'] = datetime.utcnow().isoformat()

                    redis_client.xadd(dlq_stream_name, dlq_payload, maxlen=dlq_max_len)
                    redis_client.xack(stream_name, group_name, msg_id) # Ack original message

        if not entries_with_ids:
            return {"status": "success", "processed": 0, "message": "No valid logs to process after filtering."}

        # Divide into chunks of 100 logs
        chunk_size = 100
        chunks_of_entries = [entries_with_ids[i:i + chunk_size] for i in range(0, len(entries_with_ids), chunk_size)]

        # Launch subtasks in parallel
        job = group(process_log_chunk.s([item['data'] for item in chunk], stream_name, group_name, [item['id'] for item in chunk])
                    for chunk in chunks_of_entries)
        result = job.apply_async()

        return {
            "status": "spawned_subtasks",
            "subtask_count": len(chunks_of_entries),
            "log_count": len(entries_with_ids),
            "group_id": result.id
        }

    except Exception as exc:
        logger.error(f"Master task error: {exc}")
        raise self.retry(exc=exc)