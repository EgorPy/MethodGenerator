""" Run core """

from config_generator import generate_frontend_config
from service_definitions.registry import TASKS
from service_loader import poll_tasks
from logger import logger
import asyncio


async def run():
    """ Core startup """

    logger.info("=== Core system starting ===")

    generate_frontend_config()

    if TASKS:
        logger.info(f"Loaded services: {', '.join(TASKS.keys())}")
    else:
        logger.warning("No services loaded")

    await poll_tasks()


if __name__ == "__main__":
    asyncio.run(run())
