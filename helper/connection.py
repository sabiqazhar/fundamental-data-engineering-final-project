from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# EXTRACT ENV DATABASE
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")


# LOAD ENV DATABASE
DB_USERNAME_LOAD = os.getenv("DB_USERNAME_LOAD")
DB_PASSWORD_LOAD = os.getenv("DB_PASSWORD_LOAD")
DB_HOST_LOAD = os.getenv("DB_HOST_LOAD")
DB_NAME_LOAD = os.getenv("DB_NAME_LOAD")


class Connection:
    @staticmethod
    def postgres_engine(section=None):
        """
        create connection with extract credential
        """

        try:
            engine = create_engine(
                f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
            ) if section == "extract" else create_engine(
                f"postgresql://{DB_USERNAME_LOAD}:{DB_PASSWORD_LOAD}@{DB_HOST_LOAD}/{DB_NAME_LOAD}"
            )

            return engine
        except Exception as e:
            print(f"Something wrong!!: {e}")

            return False
