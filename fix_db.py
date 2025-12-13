import sqlite3
import os

DB_FILE = "prisma_srs.db"

def fix_activity_log_table():
    """
    Ensures the 'activity_log' table exists and has the 'metadata' column.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file not found at '{os.path.abspath(DB_FILE)}'.")
        print("Please run this script from the project's root directory.")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        print("Connecting to database and starting repair...")

        # Step 1: Create the table if it doesn't exist, using the schema from app.py
        # This is idempotent and safe to run always.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                username TEXT NOT NULL, 
                action_type TEXT NOT NULL, 
                timestamp DATETIME NOT NULL
            );
        """)
        print("- 'CREATE TABLE IF NOT EXISTS' executed.")

        # Step 2: Try to add the 'metadata' column.
        # This will fail if the column already exists, which is fine.
        try:
            cursor.execute("ALTER TABLE activity_log ADD COLUMN metadata TEXT;")
            print("- 'ALTER TABLE' executed to add 'metadata' column.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("- 'metadata' column already exists. Skipping.")
            else:
                raise # Re-raise other operational errors

        conn.commit()
        print("\n✅ Tabla activity_log reparada exitosamente.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    fix_activity_log_table()
