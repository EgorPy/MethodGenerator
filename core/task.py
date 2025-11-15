""" Task class """


class Task:
    """ Generic task """

    def __init__(self, name: str):
        self.name = name

    def db_fetch(self):
        """ Override """

        raise NotImplementedError

    def set_status(self, payload, status: str):
        pass

    def save_result(self, payload, result):
        pass

    async def process(self, payload):
        """ Override """

        raise NotImplementedError
