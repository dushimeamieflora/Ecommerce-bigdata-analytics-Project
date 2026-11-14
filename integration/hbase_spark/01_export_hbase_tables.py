import json
import time
from pathlib import Path

import happybase


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

HBASE_HOST = "localhost"
HBASE_PORT = 9090

BROWSING_TABLE = "ecom:browsing_events"
PRODUCT_METRICS_TABLE = "ecom:product_metrics"

OUTPUT_DIR = Path("results/integration/hbase_export")

# Smaller batches help reduce Thrift scanner interruptions.
BATCH_SIZE = 500

# Number of attempts after a scanner interruption.
MAX_RETRIES = 10

# Display progress after this many additional rows.
PROGRESS_INTERVAL = 100_000


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def decode_bytes(value):
    """Convert HBase bytes to readable text."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def get_last_saved_row_key(output_file):
    """
    Read the last successfully saved JSONL record.
    This allows an interrupted export to resume.
    """
    if not output_file.exists() or output_file.stat().st_size == 0:
        return None

    with output_file.open("rb") as file:
        file.seek(0, 2)
        position = file.tell()

        # Move past a final newline, if present.
        if position > 0:
            file.seek(position - 1)
            if file.read(1) == b"\n":
                position -= 1

        # Find the beginning of the last line.
        while position > 0:
            file.seek(position - 1)
            if file.read(1) == b"\n":
                break
            position -= 1

        file.seek(position)
        last_line = file.readline()

    try:
        record = json.loads(last_line.decode("utf-8"))
        key = record.get("_row_key") or record.get("row_key")

        if key:
            return key.encode("utf-8")

    except (json.JSONDecodeError, UnicodeDecodeError):
        pass

    raise RuntimeError(
        f"Could not recover the last row key from {output_file}. "
        "The existing export has an incomplete or invalid final record."
    )


def build_record(row_key, columns, table_name):
    """Convert one HBase row into a JSON-compatible dictionary."""

    key_text = decode_bytes(row_key)

    record = {
        "_row_key": key_text,
        "row_key": key_text
    }

    # Decode the row key into useful fields.
    if table_name == BROWSING_TABLE:
        parts = key_text.split("#", 2)

        if len(parts) >= 1:
            record["user_id"] = parts[0]

        if len(parts) >= 2:
            record["reverse_timestamp"] = parts[1]

        if len(parts) >= 3:
            record["event_id"] = parts[2]

    elif table_name == PRODUCT_METRICS_TABLE:
        parts = key_text.split("#", 2)

        if len(parts) >= 1:
            record["product_id"] = parts[0]

        if len(parts) >= 2:
            record["metric_date"] = parts[1]

        if len(parts) >= 3:
            record["metric_hour"] = parts[2]

    # Convert HBase columns such as event:page_type
    # into JSON fields such as event_page_type.
    for column_name, value in columns.items():
        column_text = decode_bytes(column_name)
        value_text = decode_bytes(value)

        if ":" in column_text:
            family, qualifier = column_text.split(":", 1)
            field_name = f"{family}_{qualifier}"
        else:
            field_name = column_text.replace(":", "_")

        record[field_name] = value_text

    return record


# --------------------------------------------------
# HBASE EXPORT
# --------------------------------------------------

def export_hbase_table(connection, table_name, output_file):

    output_file.parent.mkdir(parents=True, exist_ok=True)

    table = connection.table(table_name)

    last_key = get_last_saved_row_key(output_file)

    if last_key:
        print(f"\nResuming {table_name} from the last saved row.")
    else:
        print(f"\nStarting export of {table_name}.")

    total_exported = 0
    retries = 0

    with output_file.open("a", encoding="utf-8") as output:

        while True:

            scan_options = {
                "batch_size": BATCH_SIZE
            }

            if last_key is not None:
                scan_options["row_start"] = last_key

            try:
                for row_key, columns in table.scan(**scan_options):

                    # HBase row_start can include the starting key.
                    # Skip it because it is already in the output.
                    if last_key is not None and row_key <= last_key:
                        continue

                    record = build_record(
                        row_key,
                        columns,
                        table_name
                    )

                    output.write(
                        json.dumps(record, ensure_ascii=False) + "\n"
                    )

                    last_key = row_key
                    total_exported += 1

                    if total_exported % PROGRESS_INTERVAL == 0:
                        output.flush()

                        print(
                            f"Exported {total_exported:,} additional rows "
                            f"from {table_name}..."
                        )

                # Reaching the end of the scan means success.
                output.flush()
                print(
                    f"Completed {table_name}: "
                    f"{total_exported:,} additional rows exported."
                )
                return total_exported

            except Exception as error:

                output.flush()
                retries += 1

                print(
                    f"\nScanner interruption in {table_name}."
                )
                print(f"Error type: {type(error).__name__}")
                print(f"Retry {retries} of {MAX_RETRIES}.")

                if retries >= MAX_RETRIES:
                    raise RuntimeError(
                        f"Export stopped after {MAX_RETRIES} retries. "
                        "Your saved JSONL data has been preserved. "
                        "Run this script again to resume."
                    ) from error

                print("Waiting 5 seconds before resuming...")
                time.sleep(5)

                # The next scan starts at the last saved row key.
                # That row is skipped to prevent duplication.


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    connection = None

    try:
        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=120_000,
            autoconnect=True
        )

        connection.open()
        print("Connected to HBase successfully.")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        browsing_file = OUTPUT_DIR / "browsing_events.jsonl"
        metrics_file = OUTPUT_DIR / "product_metrics.jsonl"

        export_hbase_table(
            connection,
            BROWSING_TABLE,
            browsing_file
        )

        export_hbase_table(
            connection,
            PRODUCT_METRICS_TABLE,
            metrics_file
        )

        print("\nHBASE EXPORT COMPLETED SUCCESSFULLY.")
        print(f"Browsing events: {browsing_file}")
        print(f"Product metrics: {metrics_file}")

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()