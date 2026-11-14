
from pathlib import Path
import logging

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
TEMP_DIR = PROJECT_DIR / "data" / "temp" / "sessions"

TEMP_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 10_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

session_files = sorted(RAW_DIR.glob("sessions_*.jsonl"))

for file_path in session_files:
    if file_path.stat().st_size == 0:
        logging.info("Skipping empty file: %s", file_path.name)
        continue

    logging.info("Splitting %s", file_path.name)

    chunk_number = 1
    record_count = 0
    output_file = None

    try:
        with file_path.open("r", encoding="utf-8") as source:
            for line in source:
                if not line.strip():
                    continue

                if record_count % CHUNK_SIZE == 0:
                    if output_file:
                        output_file.close()

                    output_name = (
                        f"{file_path.stem}_chunk_{chunk_number:03d}.jsonl"
                    )
                    output_file = (TEMP_DIR / output_name).open(
                        "w", encoding="utf-8"
                    )
                    chunk_number += 1

                output_file.write(line)
                record_count += 1

    finally:
        if output_file:
            output_file.close()

    logging.info(
        "Finished %s: %s records split into %s chunks.",
        file_path.name,
        record_count,
        chunk_number - 1
    )

logging.info("All session files have been split.")
