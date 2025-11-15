""" Image service """

from core.task import Task
from .db_methods import db_fetch_images
from .client import send_to_generator


class ImageTask(Task):
    """ Image service task """

    def db_fetch(self):
        """ Fetches required data from db """

        return db_fetch_images()

    async def handle(self, payload):
        """ Sends data to service """

        return await send_to_generator(payload)


SERVICE = ImageTask("image_service")
