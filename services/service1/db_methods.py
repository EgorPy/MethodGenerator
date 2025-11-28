from core.method_generator import AutoDB, ConnectionManager

cm = ConnectionManager()
db = AutoDB(cm.connect())
