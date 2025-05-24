import os

from dotenv import load_dotenv
from logger import LogUtils
import psycopg2

logger = LogUtils.init_logger()
logger.info("Starting the application...")

load_dotenv()
with psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
) as conn:
    logger.info("Connected to the database successfully.")


logger.info("Application finished.")
