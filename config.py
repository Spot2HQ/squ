import os
from pathlib import Path
from dotenv import dotenv_values

class Config:
    def __init__(self, sql_dir, env_path):
        self.sql_dir = Path(sql_dir)
        self.env_path = Path(env_path)
        self.config = dotenv_values(self.env_path)
        self.db_host = self.config.get("DB_HOST")
        self.db_user = self.config.get("DB_USER")
        self.db_pass = self.config.get("DB_PASSWORD")
        self.db_name = self.config.get("DB_NAME")
        self.db_port = self.config.get("DB_PORT")

    def create_mysql_uri(self, driver=None):
        if driver:
            return f"mysql+{driver}://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        else:
            return f"mysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    def create_connectorx_uri(self):
        return f"mysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    def read_sql_file(self, file_name):
        try:
            with open(self.sql_dir / file_name, "r") as file:
                query = file.read().strip()
                return query
        except FileNotFoundError:
            print(f"The file {file_name} was not found.")
            return None
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            return None
