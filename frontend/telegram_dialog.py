""" Auto generated dialog for Telegram bot """

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
import core.config as config
import aiohttp


class DynamicStates(StatesGroup):
    step = State()


class TelegramDialogManager:
    def __init__(self, bot_token: str, api_description: dict):
        self.bot = Bot(token=bot_token)
        self.dp = Dispatcher(self.bot, storage=MemoryStorage())
        self.api_description = api_description

    def register_service(self, service_name: str):
        api = self.api_description[service_name]

        @self.dp.message_handler(commands=[service_name])
        async def start_dialog(message: types.Message):
            await message.answer(f"Запуск сервиса {service_name}. Введите {', '.join(api['handle']['args'])}:")
            await DynamicStates.step.set()
            state = self.dp.current_state(user=message.from_user.id)
            await state.update_data(service=service_name)

        @self.dp.message_handler(state=DynamicStates.step)
        async def process_step(message: types.Message, state: FSMContext):
            data = await state.get_data()
            service = data['service']
            payload = {arg: message.text for arg in api['handle']['args']}
            payload['user_id'] = message.from_user.id

            # Отправка на backend API
            await self._send_to_api(service, payload)
            await message.answer("Ваш запрос принят. Как только результат будет готов, я пришлю его.")
            await state.finish()

    async def _send_to_api(self, service_name: str, payload: dict):
        url = f"http://localhost:{config.BACKEND_PORT}/api/{service_name}/handle"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload)

    def run(self):
        from aiogram import executor
        executor.start_polling(self.dp, skip_updates=True)
