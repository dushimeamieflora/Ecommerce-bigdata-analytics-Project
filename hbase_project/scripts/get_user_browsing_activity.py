
import happybase

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

HOST = "localhost"
PORT = 9090

TABLE_NAME = "ecom:browsing_events"

USER_ID = "user_000001"
LIMIT = 10


# --------------------------------------------------
# RETRIEVE USER BROWSING ACTIVITY
# --------------------------------------------------

def get_user_activity():

    connection = None

    try:
        print("Connecting to HBase...")

        connection = happybase.Connection(
            host=HOST,
            port=PORT,
            timeout=30000
        )

        connection.open()

        table = connection.table(TABLE_NAME)

        prefix = (USER_ID + "#").encode("utf-8")

        print("\nRetrieving browsing activity...")
        print("User ID:", USER_ID)
        print("Maximum records:", LIMIT)

        rows = table.scan(
            row_prefix=prefix,
            limit=LIMIT
        )

        count = 0

        print("\n========== BROWSING ACTIVITY ==========\n")

        for row_key, columns in rows:

            count += 1

            print("Record:", count)
            print("Row key:", row_key.decode("utf-8"))

            for column, value in columns.items():

                print(
                    column.decode("utf-8"),
                    ":",
                    value.decode("utf-8")
                )

            print("--------------------------------------")

        print("\nTotal records retrieved:", count)

        if count == 0:
            print("No browsing records found for this user.")

    except Exception as error:

        print("\nERROR:")
        print(type(error).__name__, error)

    finally:

        if connection:
            connection.close()
            print("\nHBase connection closed.")


if __name__ == "__main__":
    get_user_activity()
