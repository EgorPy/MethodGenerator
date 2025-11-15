""" Database with auto generated methods """

from logger import logger
import inspect
import sqlite3
import re
import os


def _guess_table_from_method(name: str) -> str:
    """ Guesses table name from method name like get_image_by_id or set_image_by_user_id -> images """

    match = re.match(r"(?:get|set|update|delete)_(\w+)_by_", name)
    if match:
        table = match.group(1)
        if not table.endswith("s"):
            table += "s"
        return table

    match = re.match(r"(?:get|set|update|delete)_(\w+)$", name)
    if match:
        table = match.group(1)
        if not table.endswith("s"):
            table += "s"
        return table
    raise AttributeError(f"Cannot guess table name. Incorrect method name: {name}")


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
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        logger.debug(f"Connected to database: {path}")

    def __getattr__(self, name: str):
        """ Dynamically create method based on its name """

        if name.startswith("set_"):
            for parser in (
                    self._parse_set_status_method,
                    self._parse_set_with_status_table,
                    self._parse_set_by_two_columns,
                    self._parse_set_by_column,
            ):
                method = parser(name)
                if method:
                    return method

        if name.startswith("get_"):
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
        _log_call_context(name)
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
            return [dict(row) for row in result]

        return method

    def _parse_get_by_column(self, name: str):
        """ get_{column}_by_{column}(value) """

        match = re.match(r"^(get|set|update|delete)_(\w+)_by_(\w+)$", name)
        if not match:
            return None

        operation, column, by_column = match.groups()
        if operation not in self.OPERATION_KEYWORDS:
            return None

        table = _guess_table_from_method(name)
        query = f"SELECT {column} FROM {table} WHERE {by_column} = ?"
        _log_call_context(name)
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
            return [dict(row) for row in result]

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
        _log_call_context(name)
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
            return [dict(row) for row in result]

        return method

    def _parse_set_with_status_table(self, name: str):
        """ set_{column}_with_{status}_{table}() or set_{column}_and_{column}_with_{status}_{table}() """

        match = re.match(r"^set_(.+)_with_(\w+)_(\w+)$", name)
        if not match:
            return None

        columns_part, status, table = match.groups()
        columns = columns_part.split("_and_")
        placeholders = ", ".join([f"{col}=?" for col in columns])
        query = f"UPDATE {table} SET {placeholders} WHERE status = ?"
        _log_call_context(name)
        logger.debug(f"Prepared SQL SET query: {query} | Status: {status}")

        def method(*values):
            """ Sets columns with status """

            _log_call_context(name)
            if len(values) != len(columns):
                raise ValueError(f"Expected {len(columns)} values, got {len(values)}")
            self._ensure_table_and_columns(table, columns + ["status"])
            with self.connection:
                self.cursor.execute(query, (*values, status))
                self.connection.commit()
                self.cursor.execute(f"SELECT * FROM {table} WHERE status = ?", (status,))
                rows = self.cursor.fetchall()
                self.cursor.execute(f"PRAGMA table_info({table})")
                col_names = [row[1] for row in self.cursor.fetchall()]
            return [dict(zip(col_names, row)) for row in rows]

        return method

    def _parse_set_by_two_columns(self, name: str):
        """ set_{column}_by_{column}_and_{column}(value, filter1, filter2) """

        match = re.match(r"^set_(\w+)_by_(\w+)_and_(\w+)$", name)
        if not match:
            return None
        column, filter1, filter2 = match.groups()
        table = _guess_table_from_method(name)
        query = f"UPDATE {table} SET {column} = ? WHERE {filter1} = ? AND {filter2}=?"
        _log_call_context(name)
        logger.debug(f"Prepared SQL query: {query}")

        def method(value_to_set, filter1_value, filter2_value):
            """ Sets columns with two filters """

            _log_call_context(name)
            self._ensure_table_and_columns(table, [column, filter1, filter2])
            with self.connection:
                self.cursor.execute(query, (value_to_set, filter1_value, filter2_value))
                self.connection.commit()
                self.cursor.execute(f"SELECT * FROM {table} WHERE {filter1}=? AND {filter2}=?", (filter1_value, filter2_value))
                rows = self.cursor.fetchall()
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in self.cursor.fetchall()]
                return [dict(zip(columns, row)) for row in rows]

        return method

    def _parse_set_by_column(self, name: str):
        """ set_{column}_by_{column}(value, filter) """

        match = re.match(r"^set_(\w+)_by_(\w+)$", name)
        if not match:
            return None
        column, by_column = match.groups()

        table = _guess_table_from_method(name)

        if column.endswith("_status"):
            column = column.removesuffix("_status")

        if by_column.endswith("_status"):
            by_column = by_column.removesuffix("_status")

        if table.endswith("_status"):
            table = table.removesuffix("_status")

            if not table.endswith("s"):
                table += "s"
            query = f"UPDATE {table} SET status=? WHERE {by_column}=?"
        else:
            if not table.endswith("s"):
                table += "s"
            query = f"UPDATE {table} SET {column}=? WHERE {by_column}=?"

        _log_call_context(name)
        logger.debug(f"Prepared SQL query: {query}")

        def method(value_to_set, filter_value):
            """ Sets columns with filter """

            _log_call_context(name)
            self._ensure_table_and_columns(table, [column, by_column, "status"])
            with self.connection:
                self.cursor.execute(query, (value_to_set, filter_value))
                self.connection.commit()

                self.cursor.execute(f"SELECT * FROM {table} WHERE {by_column}=?", (filter_value,))
                rows = self.cursor.fetchall()
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in self.cursor.fetchall()]
                return [dict(zip(columns, row)) for row in rows]

        return method

    def _parse_set_status_method(self, name: str):
        """ Parses methods like set_{table}_status(arg1, status) """

        match = re.fullmatch(r"set_(\w+)_status", name)

        if not match:
            return None

        table = match.group(1)
        if not table.endswith("s"):
            table += "s"

        query = f"UPDATE {table} SET status=? WHERE id=?"
        _log_call_context(name)
        logger.debug(f"Prepared SQL query: {query}")

        def method(id_value, status_value):
            """ Set status method """

            _log_call_context(name)
            self._ensure_table_and_columns(table, ["status"])
            with self.connection:
                self.cursor.execute(query, (status_value, id_value))
                self.connection.commit()
                self.cursor.execute(f"SELECT * FROM {table} WHERE id=?", (id_value,))
                rows = self.cursor.fetchall()
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in self.cursor.fetchall()]
                return [dict(zip(columns, row)) for row in rows]

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
                return [dict(row) for row in result]
            else:
                logger.info("Custom SQL executed successfully")
