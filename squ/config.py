import os
from pathlib import Path
from dotenv import dotenv_values

class Config:
    """
    Configuration class for managing database connection parameters and SQL files.

    Attributes:
        sql_dir (Path): Directory where SQL files are stored.
        env_path (Path): Path to the .env file containing database connection parameters.
        config (dict): Dictionary containing the environment variables from the .env file.
        db_host (str): Database host.
        db_user (str): Database user.
        db_pass (str): Database password.
        db_name (str): Database name.
        db_port (str): Database port.
        verbose (bool): Flag to control the verbosity of the output.
    """

    def __init__(self, sql_dir, env_path, verbose=False):
        """
        Initialize the Config class.

        Args:
            sql_dir (str): Directory where SQL files are stored.
            env_path (str): Path to the .env file containing database connection parameters.
            verbose (bool): Flag to control the verbosity of the output. Default is False.
        """
        self.sql_dir = Path(sql_dir)
        self.env_path = Path(env_path)
        self.config = dotenv_values(self.env_path)
        self.db_host = self.config.get("DB_HOST")
        self.db_user = self.config.get("DB_USER")
        self.db_pass = self.config.get("DB_PASSWORD")
        self.db_name = self.config.get("DB_NAME")
        self.db_port = self.config.get("DB_PORT")
        self.verbose = verbose

    def create_mysql_uri(self, driver=None):
        """
        Create a MySQL URI for connecting to the database.

        Args:
            driver (str): Optional driver for SQLAlchemy.

        Returns:
            str: MySQL URI.
        """
        if driver:
            uri = f"mysql+{driver}://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        else:
            uri = f"mysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        
        if self.verbose:
            print(f"Created MySQL URI: {uri}")
        
        return uri

    def create_connectorx_uri(self):
        """
        Create a ConnectorX URI for connecting to the database.

        Returns:
            str: ConnectorX URI.
        """
        uri = f"mysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
        
        if self.verbose:
            print(f"Created ConnectorX URI: {uri}")
        
        return uri

    def read_sql_file(self, file_name):
        """
        Read the content of a SQL file.

        Args:
            file_name (str): Name of the SQL file.

        Returns:
            str: Content of the SQL file.
        """
        try:
            with open(self.sql_dir / file_name, "r") as file:
                query = file.read().strip()
                if self.verbose:
                    print(f"Read SQL from file {file_name}: {query}")
                return query
        except FileNotFoundError:
            print(f"The file {file_name} was not found.") if self.verbose else None
            return None
        except Exception as e:
            print(f"An error occurred while reading the file: {e}") if self.verbose else None
            return None

    def execute_sql(self, sql_command, use_sqlalchemy=True):
        """
        Execute a SQL command.

        Args:
            sql_command (str): SQL command to execute.
            use_sqlalchemy (bool): Flag to determine whether to use SQLAlchemy for execution. Default is True.

        Returns:
            bool: True if the command was executed successfully, False otherwise.
        """
        db_url = self.create_mysql_uri("pymysql")
        try:
            if use_sqlalchemy:
                from sqlalchemy import create_engine
                engine = create_engine(db_url)
                with engine.connect() as connection:
                    if self.verbose:
                        print(f"Executing SQL command: {sql_command}")
                    connection.execute(sql_command)
            else:
                import pymysql
                connection = pymysql.connect(
                    host=self.db_host,
                    user=self.db_user,
                    password=self.db_pass,
                    database=self.db_name,
                    port=int(self.db_port)
                )
                with connection.cursor() as cursor:
                    if self.verbose:
                        print(f"Executing SQL command: {sql_command}")
                    cursor.execute(sql_command)
                connection.commit()
            return True
        except Exception as e:
            print(f"An error occurred while executing the command: {e}") if self.verbose else None
            return False
