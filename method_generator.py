""" Database with auto generated methods """

import sqlite3
import re


class AutoDB:
    """ Database with auto generated methods """

    def __init__(self, path="database.db"):
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()

    def __getattr__(self, name):
        """ Create method based on its name """

        # method name pattern: get_{status}_{table}_{column}()

        match = re.match(r"get_(\w+)_(\w+)_(\w+)", name)
        if not match:
            raise AttributeError(f"Unknown method: {name}")
        status, table, column = match.groups()

        def method():
            """ Function to be returned as created method """

            # Check if table exists
            self.cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            table_exists = self.cur.fetchone()

            if not table_exists:
                print(f"Requested table {table} does not exist")
                print(f"Creating table {table}")

                self.cur.execute(f"""
                    CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        status TEXT
                    )
                """)
                self.conn.commit()

            # Check if column exists
            self.cur.execute(f"PRAGMA table_info({table})")
            existing_columns = {row[1] for row in self.cur.fetchall()}
            if column not in existing_columns:
                print(f"Requested column {column} does not exist")
                print(f"Creating column {column}")

                self.cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
                self.conn.commit()

            # Running the query
            self.cur.execute(f"SELECT {column} FROM {table} WHERE status=?", (status,))
            return self.cur.fetchall()

        return method
