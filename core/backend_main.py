""" Backend API """

from fastapi import FastAPI, Form, status, Depends, HTTPException, Cookie
from fastapi.middleware.cors import CORSMiddleware
from method_generator import AutoDB, ConnectionManager
from fastapi.responses import JSONResponse
from typing_extensions import Annotated
from typing import Optional
from logger import logger
from utils import *
import threading
import logging
import uvicorn
import sqlite3
import config

app = FastAPI()
cm = ConnectionManager()


async def login_user(email: str, password: str, connection: sqlite3.Connection = Depends(cm.dependency)) -> Optional[str]:
    """ Logins user """

    db = AutoDB(connection)

    hashed_password = db.get_user_password(email=email)
    if not hashed_password or not verify_password(password, hashed_password):
        return
    user_id = db.get_user_id(email=email)

    db.delete_session(user_id=user_id)
    session_id = db.insert_session(user_id=user_id, duration=config.SESSION_DURATION)
    return session_id


@app.post("/login/", status_code=status.HTTP_200_OK)
async def login(email: Annotated[
    str, Form(min_length=5, max_length=256, pattern="^\s*[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\s*$")],
                password: Annotated[str, Form(min_length=8, max_length=256, pattern="^\S+$")],
                connection=Depends(cm.dependency)):
    """ Login endpoint """

    session_id = await login_user(email, password, connection)
    if not session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=False,  # set True in production (only HTTPS)
        samesite="strict",
        max_age=config.SESSION_DURATION,
        path="/"
    )
    return response


@app.post("/register/")
async def register(first_name: Annotated[str, Form(min_length=2, max_length=32, pattern="^[^\d]+$")],
                   last_name: Annotated[str, Form(min_length=2, max_length=32, pattern="^[^\d]+$")],
                   phone: Annotated[str, Form(min_length=4, max_length=16, pattern="^\s*\+\d+\s*$")],
                   email: Annotated[
                       str, Form(min_length=5, max_length=256, pattern="^\s*[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\s*$")],
                   password: Annotated[str, Form(min_length=8, max_length=256, pattern="^\S+$")],
                   connection: sqlite3.Connection = Depends(cm.dependency)):
    """ Register endpoint """

    db = AutoDB(connection)

    if db.is_user_exists(email=email):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT)

    db.insert_user(first_name=first_name.strip().capitalize(),
                   last_name=last_name.strip().capitalize(),
                   email=email.strip(),
                   password=hash_password(password.strip()),
                   phone=phone.strip())

    session_id = await login_user(email, password, connection)
    if not session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    response = JSONResponse(content={"message": "Registered and logged in"}, status_code=status.HTTP_201_CREATED)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        secure=False,  # set True in production (only HTTPS)
        samesite="strict",
        max_age=config.SESSION_DURATION,
        path="/"
    )
    return response


async def check_user_session(session_id: Optional[str] = Cookie(None),
                             connection: sqlite3.Connection = Depends(cm.dependency)):
    """ Checks validity of user session """

    db = AutoDB(connection)

    if not session_id:
        logging.info("No session provided")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No session provided")
    user_id = db.get_user_by_session(session_id)
    if not user_id:
        logging.info("Invalid or expired session")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired session")
    return user_id


async def check_user_session_for_logout(session_id: Optional[str] = Cookie(None),
                                        connection: sqlite3.Connection = Depends(cm.dependency)):
    """ Checks validity of user session for logout """

    db = AutoDB(connection)

    if not session_id:
        logging.info("No session provided")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No session provided")
    user_id = db.get_user_by_session(session_id)
    if not user_id:
        logging.info("Invalid or expired session")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired session")
    return session_id


@app.get("/logout/")
async def logout(session_id: str = Depends(check_user_session_for_logout),
                 connection: sqlite3.Connection = Depends(cm.dependency)):
    """ Logout endpoint """

    db = AutoDB(connection)

    logging.info(session_id)
    db.delete_session(session_id)


@app.get("/me")
async def get_me(user_id: int = Depends(check_user_session)):
    """ Check if a user is logged in endpoint """

    return {"user_id": user_id}


def start_server():
    """ Starts the server """

    logger.info(f"BACKEND server started at http://{config.DOMAIN}:{config.BACKEND_PORT}")
    uvicorn.run(app, host=config.DOMAIN, port=config.BACKEND_PORT, reload=False)


def run():
    """ Sets up the server """

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[f"http://{config.DOMAIN}:{config.FRONTEND_PORT}"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    server_thread.join()


if __name__ == '__main__':
    run()
