import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://cbu.uz/uz/arkhiv-kursov-valyut/json/"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

TG_API_ID = int(os.getenv("TG_API_ID"))
TG_API_HASH = os.getenv("TG_API_HASH")

SQL_IP = os.getenv("SQL_IP")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
