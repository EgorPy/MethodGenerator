""" Database with auto generated methods """

import sqlite3
import re


class AutoDB:
    """ Database with auto generated methods """

    operation_keywords = {"get": "get", "set": "set", "update": "update", "delete": "delete"}
    status_keywords = {"uploaded": "uploaded", "pending": "pending", "processing": "processing", "waiting": "waiting",
                       "done": "done", "error": "error"}
    query_keywords = {"by": "by", "with": "with"}

    def __init__(self, path="database.db"):
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()

    def __getattr__(self, name):
        """ Create method based on its name """

        # get_{column}_with_{status}_{table}()
        # get_{column}_and_{column}_with_{status}_{table}()
        # get_{column}_by_{column}()

        # check method name validity
        if name.find("_") == -1:
            raise AttributeError("Method name is invalid")

        # searching for operation keywords first
        operation_index = name.find("_")
        operation = name[:operation_index]
        if operation not in self.operation_keywords:
            raise AttributeError("Specified operation does not exist")

        status_index = name.find("_", operation_index + 1)
        status = name[operation_index + 1:status_index]

        if status not in self.status_keywords:
            # status is one of the table columns
            column = status
            status = ""

            query_word_index = name.find("_", status_index + 1)
            query_word = name[status_index + 1:query_word_index]

            if query_word not in self.query_keywords:
                # query word is a continuation of column name
                column = column + "_" + query_word
                query_word = ""
                print(column)
            else:
                if query_word == self.query_keywords["by"]:
                    pass
                elif query_word == self.query_keywords["with"]:
                    pass

        # uploaded
        if status == self.status_keywords["uploaded"]:
            pass
        # pending
        elif status == self.status_keywords["pending"]:
            pass
        # processing
        elif status == self.status_keywords["processing"]:
            pass
        # waiting
        elif status == self.status_keywords["waiting"]:
            pass
        # done
        elif status == self.status_keywords["done"]:
            pass
        # error
        elif status == self.status_keywords["error"]:
            pass

        # get
        if operation == self.operation_keywords["get"]:
            return  # return get method with args if they exist
        # set
        elif operation == self.operation_keywords["set"]:
            pass
        # update
        elif operation == self.operation_keywords["update"]:
            pass
        # delete
        elif operation == self.operation_keywords["delete"]:
            pass
