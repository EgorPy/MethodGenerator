from core.method_generator import AutoDB

db = AutoDB()

# print(db.get_user_image_by_user_id(123))
# db.get_discord_message_id_by_user_image_id(123)
db.execute("SELECT discord_message_id FROM user_images WHERE id = ?", (123,))
