""" Checks for waiting requests to sent them to users """

import asyncio
from core.method_generator import AutoDB
from aiogram import Bot

db = AutoDB()


class BotScheduler:
    def __init__(self, bot_token: str, services: list, interval: int = 2):
        self.bot = Bot(token=bot_token)
        self.services = services
        self.interval = interval

    async def start(self):
        while True:
            await asyncio.sleep(self.interval)
            for service in self.services:
                await self._check_service(service)

    async def _check_service(self, service_name: str):
        table = f"{service_name}_requests"
        for row in db.execute(f"SELECT id, user_id, status, image_url FROM {table}"):
            task_id, user_id, status, image_url = row
            if status == "waiting" and image_url:
                await self.bot.send_message(user_id, f"Ваш результат готов: {image_url}")
                db.execute(f"UPDATE {table} SET status = ? WHERE id = ?", ("done", task_id))
