""" Authentication, authorization, registration logic """

from backend.services.auth.logic.security import verify_password, hash_password
from backend.services.auth.schema import Sessions, Users

from core.method_generator import AutoDB
from core.config import config

from datetime import datetime, timedelta


class AuthLogic:
    @staticmethod
    async def login_user(email: str, password: str, connection):

        db = AutoDB(connection)

        result = db.select_one(Users, email=email)
        if not result:
            return None
        hashed_password = result.get("password")
        if not hashed_password or not verify_password(password, hashed_password):
            return None

        user_id = result.get("id")
        db.delete(Sessions, user_id=user_id)
        response = db.insert(Sessions, user_id=user_id, duration=config.SESSION_DURATION,
                             expires_at=str(datetime.now() + timedelta(seconds=int(config.SESSION_DURATION))))
        session_id = response.get("id")

        return session_id

    @staticmethod
    async def register_user(first_name, last_name, phone, email, password, connection):

        db = AutoDB(connection)

        result = db.select_one(Users, email=email)
        if not result:
            return None

        db.insert(
            Users,
            first_name=first_name,
            last_name=last_name,
            email=email,
            password=hash_password(password),
            phone=phone
        )

        return await AuthLogic.login_user(email, password, connection)

    @staticmethod
    async def logout_user(session_id, connection):
        """ Logout user """

        db = AutoDB(connection)
        db.delete(Sessions, id=session_id)
