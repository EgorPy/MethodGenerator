""" Image service """

from core.logger import logger
from core.task import Task
from .db_methods import *
from .client import send_to_generator


class ImageTask(Task):
    """ Image service task """

    def db_fetch(self):
        """ Fetches required data from db """

        return db.get_url_and_user_id_with_pending_images()

    async def process(self, payload):
        """ Sends data to service """

        result = await send_to_generator(payload)
        if result == "image generated":
            return "data"

    def set_status(self, payload, status: str):
        """ Sets request status """

        if not payload:
            logger.warning("Payload is empty, cannot set status")
            return

        db.set_image_status_by_user_id(status, payload[0]["user_id"])

    def save_result(self, payload, result):
        """ Saves request result """

        if not payload:
            logger.warning("Payload is empty, cannot save result")
            return

        db.set_image_by_user_id(result, payload[0]["user_id"])


SERVICE = ImageTask("image_service")
