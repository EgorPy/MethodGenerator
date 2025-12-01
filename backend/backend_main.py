""" Backend API """

from core.config import config
from core.logger import logger

from api.auth import router as auth_router

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import threading
import uvicorn
import logging

app = FastAPI()

app.include_router(auth_router, prefix="/auth", tags=["Auth"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # f"http://{config.DOMAIN}:{config.FRONTEND_PORT}"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def start_server():
    """ Starts the server """

    logger.info(f"BACKEND server started at http://{config.DOMAIN}:{config.BACKEND_PORT}")
    uvicorn.run("backend_main:app", host=config.DOMAIN, port=int(config.BACKEND_PORT), reload=False)


def run():
    """ Sets up the server """

    logger.setLevel(logging.WARNING)

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    server_thread.join()


if __name__ == '__main__':
    run()
