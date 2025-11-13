""" Database with auto generated methods """

import sqlite3
import re


def _guess_table_from_name(name):
    """ Guesses table name """

    match = re.match(r"get_(\w+)_by_", name)
    if match:
        table = match.group(1)
        if not table.endswith("s"):
            table += "s"
        return table
    return "unknown"


class AutoDB:
    """ Database with auto generated methods """

    OPERATION_KEYWORDS = {"get", "set", "update", "delete"}
    STATUS_KEYWORDS = {"uploaded", "pending", "processing", "waiting", "done", "error"}
    QUERY_KEYWORDS = {"with", "by"}

    def __init__(self, path="database.db"):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()

    def __getattr__(self, name: str):
        """ Create method based on its name """

        # check if the name suits one of those formats
        for parser in (self._parse_get_with_status_table,
                       self._parse_get_by_column,
                       self._parse_get_simple_table):
            method = parser(name)
            if method:
                return method
        raise AttributeError(f"Unknown method format: {name}")

    # ---------------- Parsers ----------------
    def _parse_get_with_status_table(self, name: str):
        # get_{column}_with_{status}_{table}()

        match = re.match(r"^(get|set|update|delete)_(.+)_with_(\w+)_(\w+)$", name)
        if not match:
            return None

        operation, columns_part, status, table = match.groups()
        if operation not in self.OPERATION_KEYWORDS or status not in self.STATUS_KEYWORDS:
            return None

        columns = columns_part.split("_and_")
        placeholders = ", ".join(columns)

        sql_query = f"SELECT {placeholders} FROM {table} WHERE status = {status}"
        print("Generated SQL query:")
        print(sql_query)

        def method():
            """ Returns column(s) with specific status """

            self._ensure_table_and_columns(table, columns)
            self.cursor.execute(f"SELECT {placeholders} FROM {table} WHERE status = ?", (status,))
            return self.cursor.fetchall()

        return method

    def _parse_get_by_column(self, name):
        # get_{column}_by_{column}()

        match = re.match(r"^(get|set|update|delete)_(\w+)_by_(\w+)$", name)
        if not match:
            return None

        operation, column, by_column = match.groups()
        if operation not in self.OPERATION_KEYWORDS:
            return None

        table = _guess_table_from_name(name)

        sql_query = f"SELECT {column} FROM {table} WHERE {by_column} = ?"
        print("Generated SQL query:")
        print(sql_query)

        def method(value):
            """ Returns column selected by another column """

            self._ensure_table_and_columns(table, [column, by_column])
            self.cursor.execute(f"SELECT {column} FROM {table} WHERE {by_column} = ?", (value,))
            return self.cursor.fetchall()

        return method

    def _parse_get_simple_table(self, name):
        """
        get_{table}()
        selects *
        """

        match = re.match(r"^(get|set|update|delete)_(\w+)$", name)
        if not match:
            return None

        operation, table = match.groups()
        if operation not in self.OPERATION_KEYWORDS:
            return None

        sql_query = f"SELECT * FROM {table}"
        print("Generated SQL query:")
        print(sql_query)

        def method():
            """ Returns every column from the table """

            self._ensure_table_and_columns(table, [])
            self.cursor.execute(f"SELECT * FROM {table}")
            return self.cursor.fetchall()

        return method

    # ---------------- Utils ----------------
    def _ensure_table_and_columns(self, table, columns):
        """ Checks if tables and columns exist and creates them if not """

        with self.connection:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,))
            if not self.cursor.fetchone():
                print(f"Table {table} does not exist")
                self._create_table(table)

            self.cursor.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in self.cursor.fetchall()}
            for column in columns:
                if column not in existing:
                    print(f"Column {column} does not exist")
                    print(f"Creating column {column}:")
                    sql_query = f"ALTER TABLE {table} ADD COLUMN {column} TEXT"
                    print(sql_query)
                    self.cursor.execute(sql_query)

    def _create_table(self, table):
        """ Creates table """

        print(f"Creating table {table}:")

        sql_query = f"""CREATE TABLE {table} (
    id INTEGER PRIMARY KEY AUTOINCREMENT
)"""
        print(sql_query)

        with self.connection:
            self.cursor.execute(sql_query)
