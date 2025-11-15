""" Task class """

from logger import logger


class Task:
    """ Generic task """

    def __init__(self, name: str):
        self.name = name

    def db_fetch(self):
        """ Override """

        raise NotImplementedError

    async def handle(self, payload):
        """ Override """

        raise NotImplementedError
