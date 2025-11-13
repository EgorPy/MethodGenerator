from method_generator import AutoDB

db = AutoDB()

# need to figure out can I parse all of these methods

db.get_first_pending_image_request() # get = keyword, first = keyword, pending = status, image_request = column
db.get_discord_message_id_by_user_image_id(123) # discord_message_id = column, by = keyword, user_image_id = column
db.get_image(123) # image means from table images

# table users
# id
# user_id
# birthday

# table images
# id
# user_id
# image

# table videos
# id
# user_id
# video

db.get_user_ids_with_pending_videos() # from table videos
db.get_user_ids_with_pending_images() # from table images
db.get_user_ids_and_birthdays_with_pending_images() # from table images
db.get_image_by_user_id(123) # from table images

# get_{column}_with_{status}_{table}()
# get_{column}_and_{column}_with_{status}_{table}()
# get_{column}_by_{column}()
