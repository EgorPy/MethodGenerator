""" Run core """

from config_generator import generate_frontend_config
from service_loader import poll_tasks
from registry import TASKS
from logger import logger

import asyncio
import logging


async def run():
    """ Core startup """

    logger.setLevel(logging.DEBUG)

    logger.info("=== Core system started ===")

    generate_frontend_config()

    if TASKS:
        for service in TASKS.keys():
            logger.info(f"Loaded service: {service}")
    else:
        logger.warning("No services loaded")

    await poll_tasks()


if __name__ == "__main__":
    asyncio.run(run())
