from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from .config import (
    LMS_DB_HOST,
    LMS_DB_PORT,
    LMS_DB_NAME,
    LMS_DB_USER,
    LMS_DB_PASS,
    MOODLE_DB_HOST,
    MOODLE_DB_PORT,
    MOODLE_DB_NAME,
    MOODLE_DB_USER,
    MOODLE_DB_PASS,
)


def _mysql_url(host: str, port: int, db: str, user: str, pwd: str) -> str:
    safe_pwd = quote_plus(pwd)
    safe_user = quote_plus(user)
    return f"mysql+pymysql://{safe_user}:{safe_pwd}@{host}:{port}/{db}"


LMS_ENGINE: Engine = create_engine(
    _mysql_url(LMS_DB_HOST, LMS_DB_PORT, LMS_DB_NAME, LMS_DB_USER, LMS_DB_PASS),
    pool_pre_ping=True,
)

MOODLE_ENGINE: Engine = create_engine(
    _mysql_url(
        MOODLE_DB_HOST, MOODLE_DB_PORT, MOODLE_DB_NAME, MOODLE_DB_USER, MOODLE_DB_PASS
    ),
    pool_pre_ping=True,
)
