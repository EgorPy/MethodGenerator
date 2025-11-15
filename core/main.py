""" Run core """

import asyncio
from core.logger import logger
from core.service_loader import poll_tasks
from service_definitions.registry import TASKS


async def main():
    logger.info("=== Core system starting ===")

    if TASKS:
        logger.info(f"Loaded services: {', '.join(TASKS.keys())}")
    else:
        logger.warning("No services loaded")

    await poll_tasks()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("=== Core stopped manually ===")
