import logging

from core.method_generator import ConnectionManager, AutoDB
from core.logger import logger

logger.setLevel(logging.DEBUG)

cm = ConnectionManager(path="../database.db")
db = AutoDB(cm.connect())

# db.delete_user(email="user@gmail.com")
print(db.is_user_exists(email="user@gmail.com"))
