""" Database with auto generated methods """

import sqlite3
import logging
import re

logger = logging.getLogger("AutoDB")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def _guess_table_from_name(name: str) -> str:
    """ Guesses table name from method name like get_image_by_id -> images """

    match = re.match(r"get_(\w+)_by_", name)
    if match:
        table = match.group(1)
        if not table.endswith("s"):
            table += "s"
        return table
    return "unknown"


class AutoDB:
    """ Database with auto-generated methods and logging """

    OPERATION_KEYWORDS = {"get", "set", "update", "delete"}
    STATUS_KEYWORDS = {"uploaded", "pending", "processing", "waiting", "done", "error"}
    QUERY_KEYWORDS = {"with", "by"}

    def __init__(self, path="database.db"):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        logger.debug(f"Connected to database: {path}")

    def __getattr__(self, name: str):
        """ Dynamically create method based on its name """

        for parser in (
                self._parse_get_with_status_table,
                self._parse_get_by_column,
                self._parse_get_simple_table
        ):
            method = parser(name)
            if method:
                return method

        raise AttributeError(f"Unknown method format: {name}")

    # ------------------ Parsers ------------------
    def _parse_get_with_status_table(self, name: str):
        """ get_{column}_with_{status}_{table}() or get_{column}_and_{column}_with_{status}_{table}() """

        match = re.match(r"^(get|set|update|delete)_(.+)_with_(\w+)_(\w+)$", name)
        if not match:
            return None

        operation, columns_part, status, table = match.groups()
        if operation not in self.OPERATION_KEYWORDS or status not in self.STATUS_KEYWORDS:
            return None

        columns = columns_part.split("_and_")
        placeholders = ", ".join(columns)
        query = f"SELECT {placeholders} FROM {table} WHERE status = ?"
        logger.debug(f"Prepared SQL query: {query} | Status: {status}")

        def method():
            """ Returns column(s) with specific status """

            self._ensure_table_and_columns(table, columns)
            logger.debug(f"Executing query with status={status}")
            with self.connection:
                self.cursor.execute(query, (status,))
                result = self.cursor.fetchall()
            logger.info(f"Returned {len(result)} rows for columns: {columns}")
            return result

        return method

    def _parse_get_by_column(self, name: str):
        """ get_{column}_by_{column}(value) """

        match = re.match(r"^(get|set|update|delete)_(\w+)_by_(\w+)$", name)
        if not match:
            return None

        operation, column, by_column = match.groups()
        if operation not in self.OPERATION_KEYWORDS:
            return None

        table = _guess_table_from_name(name)
        query = f"SELECT {column} FROM {table} WHERE {by_column} = ?"
        logger.debug(f"Prepared SQL query: {query}")

        def method(value):
            """ Returns column selected by another column """

            self._ensure_table_and_columns(table, [column, by_column])
            with self.connection:
                logger.debug(f"Executing query with {by_column}={value}")
                self.cursor.execute(query, (value,))
                result = self.cursor.fetchall()
            logger.info(f"Returned {len(result)} rows for column: {column}")
            return result

        return method

    def _parse_get_simple_table(self, name: str):
        """ get_{table}() -> SELECT * FROM table """

        match = re.match(r"^(get|set|update|delete)_(\w+)$", name)
        if not match:
            return None

        operation, table = match.groups()
        if operation not in self.OPERATION_KEYWORDS:
            return None

        query = f"SELECT * FROM {table}"
        logger.debug(f"Prepared SQL query: {query}")

        def method():
            """ Returns all columns from the table """

            self._ensure_table_and_columns(table, [])
            with self.connection:
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in self.cursor.fetchall()]
            logger.info(f"Returned {len(result)} rows with columns: {columns}")
            return result

        return method

    # ------------------ Utilities ------------------
    def _ensure_table_and_columns(self, table: str, columns: list):
        """ Checks if table and columns exist and creates them if not """

        with self.connection:
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
            )
            if not self.cursor.fetchone():
                logger.warning(f"Table '{table}' does not exist. Creating...")
                self._create_table(table)

            self.cursor.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in self.cursor.fetchall()}
            for column in columns:
                if column not in existing:
                    logger.warning(f"Column '{column}' does not exist in '{table}'. Creating...")
                    sql = f"ALTER TABLE {table} ADD COLUMN {column} TEXT"
                    logger.debug(f"Executing SQL: {sql}")
                    self.cursor.execute(sql)

    def _create_table(self, table: str):
        """ Creates a table with just an id column """

        sql = f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        logger.debug(f"Creating table '{table}' with SQL: {sql}")
        self.cursor.execute(sql)

    def execute(self, sql: str, params: tuple = None):
        """ Execute a custom SQL query with optional parameters """

        logger.debug(f"Executing custom SQL: {sql}")
        if params:
            logger.debug(f"With parameters: {params}")

        with self.connection:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)

            if sql.strip().lower().startswith("select"):
                result = self.cursor.fetchall()
                logger.info(f"Custom SQL returned {len(result)} rows")
                return result
            else:
                logger.info("Custom SQL executed successfully")
