"""
Abstract pipeline to connect and manage independent services (business logic)
"""

from service_definitions.registry import TASKS
from logger import logger
from task import Task
import asyncio
import config


def process_task(task: Task, payload):
    logger.debug(f"Processing task: {task.name} | payload={payload}")


async def poll_tasks():
    """
    Periodically checks all task types for pending jobs and processes them.
    """

    logger.info("Task polling started")

    while True:
        await asyncio.sleep(config.REQUEST_INTERVAL)

        for task in TASKS.values():
            payload = task.db_fetch()

            if payload:
                logger.info(f"Task '{task.name}' has work: {payload}")
                asyncio.create_task(process_task(task, payload))
