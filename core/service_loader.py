"""
Abstract pipeline to connect and manage independent services (business logic)
"""

from service_definitions.registry import TASKS
from logger import logger
from task import Task
import asyncio
import config


async def process_task(task: Task, payload):
    """ Processes generic task """

    logger.debug(f"Processing task: {task.name} | payload={payload}")

    task.set_status(payload, "processing")

    try:
        result = await task.process(payload)
        task.save_result(payload, result)
        task.set_status(payload, "waiting")
        logger.info(f"Task '{task.name}' processed, waiting for user delivery")
    except Exception as e:
        logger.error(f"Task '{task.name}' failed: {e}")
        task.set_status(payload, "error")


async def poll_tasks():
    """
    Periodically checks all task types for pending jobs and processes them.
    """

    logger.info("Task polling started")

    while True:
        for task in TASKS.values():
            payload = task.db_fetch()

            if payload:
                logger.info(f"Task '{task.name}' has work: {payload}")
                asyncio.create_task(process_task(task, payload))

        await asyncio.sleep(config.REQUEST_INTERVAL)
