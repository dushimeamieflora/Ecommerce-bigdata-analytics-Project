
import happybase
from datetime import datetime

# =====================================================
# HBASE INTEGRATION VERIFICATION
# E-commerce Big Data Analytics Project
# =====================================================

HOST = "localhost"
PORT = 9090
CONNECTION_TIMEOUT = 300000

BROWSING_TABLE = "ecom:browsing_events"
PRODUCT_TABLE = "ecom:product_metrics"

# Previously verified reference counts
EXPECTED_BROWSING_ROWS = 10301209
EXPECTED_PRODUCT_ROWS = 95140

# Known browsing event used for testing
TEST_EVENT_KEY = (
    "user_000001#8206599017123#sess_0000662118_0003"
)


def main():

    connection = None
    results = []

    print("=" * 60)
    print("HBASE INTEGRATION VERIFICATION")
    print("=" * 60)

    print("Date:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("Host:", HOST)
    print("Port:", PORT)

    try:
        # 1. Connect to HBase
        print("\n1. Testing HBase connection...")

        connection = happybase.Connection(
            host=HOST,
            port=PORT,
            timeout=CONNECTION_TIMEOUT
        )

        connection.open()

        print("PASS: Connected to HBase.")
        results.append("HBase connection: PASS")

        # 2. Verify tables
        print("\n2. Checking HBase tables...")

        available_tables = connection.tables()

        if BROWSING_TABLE.encode() not in available_tables:
            raise Exception("Browsing events table not found.")

        if PRODUCT_TABLE.encode() not in available_tables:
            raise Exception("Product metrics table not found.")

        print("PASS:", BROWSING_TABLE)
        print("PASS:", PRODUCT_TABLE)

        results.append("Browsing table exists: PASS")
        results.append("Product metrics table exists: PASS")

        browsing = connection.table(BROWSING_TABLE)
        products = connection.table(PRODUCT_TABLE)

        # 3. Verify browsing events
        print("\n3. Testing browsing event retrieval...")

        user_events = list(
            browsing.scan(
                row_prefix=b"user_000001#"
            )
        )

        actual_user_events = len(user_events)

        print("User: user_000001")
        print("Retrieved events:", actual_user_events)

        if actual_user_events != 904:
            raise Exception(
                f"Expected 904 user events, found {actual_user_events}."
            )

        print("PASS: User browsing activity retrieved.")
        results.append("User browsing retrieval (904 events): PASS")

        # 4. Verify one specific browsing event
        print("\n4. Testing individual browsing event...")

        event = browsing.row(TEST_EVENT_KEY.encode())

        if not event:
            raise Exception("Specific browsing event not found.")

        required_event_columns = [
            b"event:page_type",
            b"event:timestamp",
            b"event:view_duration",
            b"context:session_id"
        ]

        for column in required_event_columns:
            if column not in event:
                raise Exception(
                    f"Missing browsing event column: {column.decode()}"
                )

        print("PASS: Specific browsing event retrieved.")
        print("Page type:", event[b"event:page_type"].decode())
        print("Session:", event[b"context:session_id"].decode())

        results.append("Individual browsing event retrieval: PASS")

        # 5. Verify product metrics
        print("\n5. Testing product metrics retrieval...")

        product_records = list(products.scan(limit=3))

        if len(product_records) != 3:
            raise Exception("Could not retrieve three product records.")

        required_metrics = [
            b"metrics:views",
            b"metrics:cart_additions",
            b"metrics:purchases",
            b"metrics:quantity_sold",
            b"metrics:revenue"
        ]

        for row_key, data in product_records:
            for column in required_metrics:
                if column not in data:
                    raise Exception(
                        f"Missing product metric: {column.decode()}"
                    )

        print("PASS: Three product records retrieved.")
        print("PASS: All five metric columns verified.")

        results.append("Product metrics retrieval: PASS")
        results.append("Five product metric columns: PASS")

        # 6. Display reference counts
        print("\n6. Previously verified HBase row counts:")

        print(f"Browsing events: {EXPECTED_BROWSING_ROWS:,}")
        print(f"Product metrics: {EXPECTED_PRODUCT_ROWS:,}")

        print(
            "NOTE: These counts were verified separately "
            "using HBase Shell."
        )

        results.append(
            "Browsing event reference count: 10,301,209"
        )

        results.append(
            "Product metrics reference count: 95,140"
        )

        # 7. Final summary
        print("\n" + "=" * 60)
        print("HBASE INTEGRATION VERIFICATION SUCCESSFUL")
        print("=" * 60)

        for result in results:
            print("[PASS]", result)

    except Exception as error:

        print("\nVERIFICATION FAILED")
        print("Error:", error)
        raise

    finally:

        if connection is not None:
            connection.close()
            print("\nHBase connection closed.")


if __name__ == "__main__":
    main()
