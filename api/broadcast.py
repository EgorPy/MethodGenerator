""" Mass message API """

from fastapi import APIRouter
from core.method_generator import AutoDB
from aiogram import Bot

router = APIRouter()
db = AutoDB()


@router.post("/broadcast")
async def broadcast_message(bot_token: str, message_text: str):
    bot = Bot(token=bot_token)
    user_ids = [row[0] for row in db.get_user_id_from_users()]
    for user_id in user_ids:
        await bot.send_message(user_id, message_text)
    return {"sent_to": len(user_ids)}
