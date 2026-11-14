
import glob
import json
import os
import time
from datetime import datetime, timezone

import happybase


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

DATA_PATTERN = "data/raw/sessions_*.jsonl"

CHECKPOINT_DIR = "hbase_project/checkpoints"
CHECKPOINT_FILE = os.path.join(
    CHECKPOINT_DIR,
    "browsing_events_completed.txt"
)

HOST = "localhost"
PORT = 9090
CONNECTION_TIMEOUT = 300000

TABLE_NAME = "ecom:browsing_events"

BATCH_SIZE = 25
MAX_RETRIES = 5

os.makedirs(CHECKPOINT_DIR, exist_ok=True)


# ============================================================
# 2. CONNECTION TO HBASE
# ============================================================

def connect_to_hbase():
    """Connect to HBase and return the connection and table."""

    connection = happybase.Connection(
        host=HOST,
        port=PORT,
        timeout=CONNECTION_TIMEOUT,
        autoconnect=False
    )

    connection.open()

    table = connection.table(TABLE_NAME)

    return connection, table


# ============================================================
# 3. CHECKPOINT MANAGEMENT
# ============================================================

def load_completed_files():
    """
    Load completed filenames from the checkpoint.

    Only filenames are compared, avoiding Windows/Linux
    path separator differences.
    """

    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    completed = set()

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            filename = line.strip()

            if filename:
                filename = filename.replace("\\", "/")
                completed.add(os.path.basename(filename))

    return completed


def mark_file_completed(filename):
    """Record a file after all its events have been written."""

    filename = os.path.basename(filename.replace("\\", "/"))

    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as file:
        file.write(filename + "\n")


# ============================================================
# 4. RELIABLE BATCH WRITING
# ============================================================

def send_chunk(rows, connection, table):
    """
    Send a small batch to HBase.

    On failure:
    - Close the old connection.
    - Wait before retrying.
    - Establish a new connection.
    - Replay the same rows.

    Deterministic row keys make replay idempotent.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            batch = table.batch(batch_size=BATCH_SIZE)

            for row_key, cells in rows:
                batch.put(row_key, cells)

            batch.send()

            return connection, table

        except Exception as error:

            print(
                f"\nBatch write failed "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )

            print(f"Error: {error}")

            try:
                connection.close()
            except Exception:
                pass

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    "Batch failed after all retry attempts. "
                    "Run the script again to resume."
                ) from error

            wait_seconds = min(2 ** attempt, 30)

            print(
                f"Waiting {wait_seconds} seconds "
                "before reconnecting..."
            )

            time.sleep(wait_seconds)

            connection, table = connect_to_hbase()

    return connection, table


# ============================================================
# 5. PROCESS ONE JSONL FILE
# ============================================================

def process_file(filename, connection, table):
    """
    Read one JSONL file line by line and load its page views.

    The entire file is never loaded into memory.
    """

    rows = []

    events_written = 0
    invalid_lines = 0
    skipped_events = 0

    print(f"\nProcessing: {filename}")

    with open(filename, "r", encoding="utf-8") as file:

        for line_number, line in enumerate(file, start=1):

            try:
                record = json.loads(line)

            except json.JSONDecodeError:
                invalid_lines += 1
                continue

            user_id = record.get("user_id")
            session_id = record.get("session_id")
            page_views = record.get("page_views", [])

            if (
                not user_id
                or not session_id
                or not isinstance(page_views, list)
            ):
                skipped_events += 1
                continue

            for index, event in enumerate(page_views):

                if not isinstance(event, dict):
                    skipped_events += 1
                    continue

                timestamp = event.get("timestamp")

                if not timestamp or not isinstance(timestamp, str):
                    skipped_events += 1
                    continue

                try:
                    event_time = datetime.fromisoformat(
                        timestamp.replace("Z", "+00:00")
                    )

                    if event_time.tzinfo is None:
                        event_time = event_time.replace(
                            tzinfo=timezone.utc
                        )

                    timestamp_ms = int(
                        event_time.timestamp() * 1000
                    )

                except (ValueError, TypeError, OverflowError):
                    skipped_events += 1
                    continue

                reverse_timestamp = (
                    9999999999999 - timestamp_ms
                )

                event_id = f"{session_id}_{index:04d}"

                row_key = (
                    f"{user_id}#"
                    f"{reverse_timestamp:013d}#"
                    f"{event_id}"
                ).encode("utf-8")

                cells = {
                    b"context:session_id": str(
                        session_id
                    ).encode("utf-8")
                }

                optional_fields = {
                    b"event:event_type": event.get("event_type"),
                    b"event:product_id": event.get("product_id"),
                    b"event:category_id": event.get("category_id"),
                    b"event:timestamp": timestamp,
                    b"context:device": record.get("device"),
                    b"context:source": record.get("source"),
                }

                for column, value in optional_fields.items():

                    if value is not None and value != "":
                        cells[column] = str(value).encode("utf-8")

                rows.append((row_key, cells))

                # Send small batches to HBase.
                if len(rows) >= BATCH_SIZE:

                    connection, table = send_chunk(
                        rows,
                        connection,
                        table
                    )

                    events_written += len(rows)

                    rows.clear()

                    if events_written % 10000 == 0:
                        print(
                            f"Events written from this file: "
                            f"{events_written:,}"
                        )

        # Send the remaining events.
        if rows:

            connection, table = send_chunk(
                rows,
                connection,
                table
            )

            events_written += len(rows)

            rows.clear()

    print(f"\nFile completed: {filename}")

    print(f"Events written: {events_written:,}")
    print(f"Invalid JSON lines skipped: {invalid_lines:,}")
    print(f"Events skipped: {skipped_events:,}")

    return connection, table, events_written


# ============================================================
# 6. MAIN EXECUTION
# ============================================================

def main():

    files = sorted(glob.glob(DATA_PATTERN))

    if not files:
        print("No session files found.")
        return

    completed = load_completed_files()

    all_filenames = {
        os.path.basename(filename.replace("\\", "/"))
        for filename in files
    }

    # Show checkpoint information.
    valid_completed = completed.intersection(all_filenames)

    print(f"Session files found: {len(files)}")
    print(f"Completed files recorded: {len(completed)}")
    print(f"Completed files matching this dataset: {len(valid_completed)}")

    # Show the next files that need processing.
    pending_files = [
        filename
        for filename in files
        if os.path.basename(filename.replace("\\", "/"))
        not in completed
    ]

    if not pending_files:
        print("\nAll session files are already completed.")
        return

    print("\nNext files to process:")

    for filename in pending_files[:5]:
        print(f"  {os.path.basename(filename)}")

    connection = None
    total_written = 0

    try:

        connection, table = connect_to_hbase()

        print("\nConnected to HBase.")

        for filename in files:

            file_name = os.path.basename(
                filename.replace("\\", "/")
            )

            # Skip files already completed.
            if file_name in completed:

                print(f"Skipping completed file: {file_name}")
                continue

            # Skip and record empty files.
            if os.path.getsize(filename) == 0:

                print(f"Skipping empty file: {file_name}")

                mark_file_completed(filename)

                completed.add(file_name)

                continue

            # Process the next unfinished file.
            connection, table, count = process_file(
                filename,
                connection,
                table
            )

            total_written += count

            # Save progress only after the entire file succeeds.
            mark_file_completed(filename)

            completed.add(file_name)

            print(f"Checkpoint saved: {file_name}")

            print(
                f"Total events written in this run: "
                f"{total_written:,}"
            )

        print("\nAll session files have been processed.")

        print(
            f"Total events written in this run: "
            f"{total_written:,}"
        )

    finally:

        if connection is not None:

            try:
                connection.close()

            except Exception:
                pass


# ============================================================
# 7. RUN PROGRAM
# ============================================================

if __name__ == "__main__":
    main()
