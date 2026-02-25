import os
from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value is not None else default


LMS_DB_HOST = _env("LMS_DB_HOST", "127.0.0.1")
LMS_DB_PORT = int(_env("LMS_DB_PORT", "3306"))
LMS_DB_NAME = _env("LMS_DB_NAME", "lms")
LMS_DB_USER = _env("LMS_DB_USER", "demo")
LMS_DB_PASS = _env("LMS_DB_PASS", "Demo@123")

MOODLE_DB_HOST = _env("MOODLE_DB_HOST", "127.0.0.1")
MOODLE_DB_PORT = int(_env("MOODLE_DB_PORT", "3306"))
MOODLE_DB_NAME = _env("MOODLE_DB_NAME", "moodle")
MOODLE_DB_USER = _env("MOODLE_DB_USER", "demo")
MOODLE_DB_PASS = _env("MOODLE_DB_PASS", "Demo@123")
MOODLE_DB_PREFIX = _env("MOODLE_DB_PREFIX", "mdl_")
