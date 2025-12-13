import sqlite3
import json
import os

# --- Configuration ---
# The database file used by the Streamlit app.
DB_FILE = "prisma_srs.db"

def verify_logs():
    """
    Connects to the database and prints the last 5 entries from activity_log.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file not found at '{os.path.abspath(DB_FILE)}'")
        print("Please ensure this script is run from the project's root directory (/Users/danielsuarezsucre/cqomunity).")
        return

    conn = None
    try:
        print(f"Connecting to database: {DB_FILE}...")
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("\n--- Verifying last 5 events in activity_log ---")
        query = "SELECT * FROM activity_log ORDER BY id DESC LIMIT 5"
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("No events found in activity_log.")
            print("Hint: Try interacting with the app first to generate some answer events.")
            return
        
        print(f"Found {len(rows)} event(s).")
        print("-" * 50)

        for i, row in enumerate(rows):
            print(f"EVENT #{i+1} (ID: {row['id']})")
            print(f"  Timestamp: {row['timestamp']}")
            print(f"  User:      {row['username']}")
            print(f"  Action:    {row['action_type']}")
            
            metadata_str = row['metadata']
            if metadata_str:
                try:
                    metadata_dict = json.loads(metadata_str)
                    pretty_metadata = json.dumps(metadata_dict, indent=4)
                    print(f"  Metadata:  \n{pretty_metadata}")
                except (json.JSONDecodeError, TypeError):
                    print(f"  Metadata (raw): {metadata_str}")
            else:
                print("  Metadata:  (empty)")
            
            print("-" * 50)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    verify_logs()
