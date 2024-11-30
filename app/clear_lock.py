# app/clear_db_lock.py
import os
from pathlib import Path
import duckdb
import time


def clear_database_lock():
    # Path to the database
    db_path = Path(__file__).parent.parent / "odi_data.db"
    lock_path = db_path.parent / (db_path.name + ".lock")

    print(f"Checking database at: {db_path}")

    # First try to close any open connections
    try:
        conn = duckdb.connect(str(db_path))
        conn.close()
        print("Closed any open connections")
    except:
        print("No active connections found")

    # Remove the lock file if it exists
    if lock_path.exists():
        try:
            lock_path.unlink()
            print(f"Removed lock file: {lock_path}")
        except Exception as e:
            print(f"Error removing lock file: {e}")

    # Remove the database file itself if needed
    if db_path.exists():
        try:
            db_path.unlink()
            print(f"Removed database file: {db_path}")
        except Exception as e:
            print(f"Error removing database file: {e}")

    print("Database locks cleared")


if __name__ == "__main__":
    clear_database_lock()