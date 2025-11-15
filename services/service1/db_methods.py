from core.method_generator import AutoDB

db = AutoDB()


def db_fetch_images():
    return db.get_url_and_user_id_with_pending_images()
