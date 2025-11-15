""" Database with auto generated methods """

from core.logger import logger
import inspect
import sqlite3
import re
import os


def _guess_table_from_name(name: str) -> str:
    """ Guesses table name from method name like get_image_by_id -> images """

    match = re.match(r"get_(\w+)_by_", name)
    if match:
        table = match.group(1)
        if not table.endswith("s"):
            table += "s"
        return table
    raise AttributeError("Cannot guess table name. Incorrect method name")


def _log_call_context(method_name: str):
    """ Log method name, line and file """

    frame = inspect.stack()[2]  # calling method
    filename = os.path.basename(frame.filename)
    lineno = frame.lineno
    func = frame.function

    logger.debug(f"Generated method '{method_name}' called from {filename}:{lineno} in {func}()")


class AutoDB:
    """
    Database with auto-generated methods and logging
    Method-Driven Data Modeling (MDDM) or
    Code-Driven Data Definition (CDDD)
    """

    OPERATION_KEYWORDS = {"get", "set", "update", "delete"}
    STATUS_KEYWORDS = {"uploaded", "pending", "processing", "waiting", "done", "error"}
    QUERY_KEYWORDS = {"with", "by"}

    def __init__(self, path="../database.db"):
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

            _log_call_context(name)
            self._ensure_table_and_columns(table, columns + ["status"])
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

            _log_call_context(name)
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

            _log_call_context(name)
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

        sql_lower = sql.lower()

        # ------------------------------
        # 1. Detect table names
        # ------------------------------
        tables = set()

        # SELECT ... FROM table
        m = re.findall(r"from\s+(\w+)", sql_lower)
        tables.update(m)

        # JOIN table
        m = re.findall(r"join\s+(\w+)", sql_lower)
        tables.update(m)

        # INSERT INTO table
        m = re.findall(r"insert\s+into\s+(\w+)", sql_lower)
        tables.update(m)

        # UPDATE table
        m = re.findall(r"update\s+(\w+)", sql_lower)
        tables.update(m)

        # DELETE FROM table
        m = re.findall(r"delete\s+from\s+(\w+)", sql_lower)
        tables.update(m)

        # ------------------------------
        # 2. Ensure tables exist
        # ------------------------------
        for table in tables:
            with self.connection:
                self.cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
                )
                if not self.cursor.fetchone():
                    logger.warning(f"[execute] Table '{table}' does not exist. Creating...")
                    self._create_table(table)

        # ------------------------------
        # 3. Detect columns from SELECT/WHERE
        # ------------------------------
        for table in tables:
            self.cursor.execute(f"PRAGMA table_info({table})")
            existing_cols = {row[1] for row in self.cursor.fetchall()}

            # SELECT column1, column2 FROM table
            m = re.search(r"select\s+(.*?)\s+from", sql_lower)
            if m:
                raw = m.group(1)
                if raw.strip() != "*" and "(" not in raw:
                    cols = [c.strip() for c in raw.split(",")]
                    for col in cols:
                        if col not in existing_cols:
                            logger.warning(f"[execute] Column '{col}' does not exist in '{table}'. Creating...")
                            self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
                            existing_cols.add(col)

            # WHERE column = ?
            m = re.findall(r"where\s+(\w+)\s*=", sql_lower)
            for col in m:
                if col not in existing_cols:
                    logger.warning(f"[execute] Column '{col}' does not exist in '{table}'. Creating...")
                    self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
                    existing_cols.add(col)

            # UPDATE table SET column = ...
            m = re.findall(r"set\s+(\w+)\s*=", sql_lower)
            for col in m:
                if col not in existing_cols:
                    logger.warning(f"[execute] Column '{col}' does not exist in '{table}'. Creating...")
                    self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
                    existing_cols.add(col)

            # INSERT INTO table (col1, col2, ...)
            m = re.search(r"insert\s+into\s+\w+\s*\((.*?)\)", sql_lower)
            if m:
                cols = [c.strip() for c in m.group(1).split(",")]
                for col in cols:
                    if col not in existing_cols:
                        logger.warning(f"[execute] Column '{col}' does not exist in '{table}'. Creating...")
                        self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
                        existing_cols.add(col)

        # ------------------------------
        # 4. Run actual query
        # ------------------------------
        with self.connection:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)

            if sql_lower.strip().startswith("select"):
                result = self.cursor.fetchall()
                logger.info(f"Custom SQL returned {len(result)} rows")
                return result
            else:
                logger.info("Custom SQL executed successfully")
