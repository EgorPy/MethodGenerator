""" Task class """


class Task:
    """ Generic task """

    def __init__(self, name: str):
        self.name = name

    def db_fetch(self):
        """ Override """

        raise NotImplementedError

    def set_status(self, payload, status: str):
        """ Override """

        raise NotImplementedError

    def save_result(self, payload, result):
        """ Override """

        raise NotImplementedError

    async def process(self, payload):
        """ Override """

        raise NotImplementedError
