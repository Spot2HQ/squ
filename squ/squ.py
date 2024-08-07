import importlib
from .config import Config

class SQU:
    """
    Main class for managing database operations using different libraries (pandas, polars, dask).

    Attributes:
        config (Config): Configuration object.
        verbose (bool): Flag to control the verbosity of the output.
    """

    def __init__(self, env_path, sql_dir=None, verbose=False):
        """
        Initialize the SQU class.

        Args:
            env_path (str): Path to the .env file containing database connection parameters.
            sql_dir (str, optional): Directory where SQL files are stored. Default is None.
            verbose (bool): Flag to control the verbosity of the output. Default is False.
        """
        self.config = Config(sql_dir, env_path, verbose) if sql_dir else None
        self.verbose = verbose

    def qpd(self, sql):
        """
        Execute a SQL query and return the result as a pandas DataFrame.

        Args:
            sql (str): SQL query or file name containing the query.

        Returns:
            pandas.DataFrame: Result of the query.
        """
        return self._execute_query(sql, 'pandas')

    def qpl(self, sql):
        """
        Execute a SQL query and return the result as a polars DataFrame.

        Args:
            sql (str): SQL query or file name containing the query.

        Returns:
            polars.DataFrame: Result of the query.
        """
        return self._execute_query(sql, 'polars')

    def qdd(self, sql):
        """
        Execute a SQL query and return the result as a dask DataFrame.

        Args:
            sql (str): SQL query or file name containing the query.

        Returns:
            dask.DataFrame: Result of the query.
        """
        return self._execute_query(sql, 'dask')

    def _execute_query(self, sql, library):
        """
        Execute a SQL query using the specified library.

        Args:
            sql (str): SQL query or file name containing the query.
            library (str): Library to use for executing the query ('pandas', 'polars', 'dask').

        Returns:
            DataFrame: Result of the query.
        """
        sql_query = self._get_query(sql)
        if sql_query is None:
            return None

        if library == 'pandas':
            db_url = self.config.create_mysql_uri("pymysql")
            return self._execute_with_pandas(db_url, sql_query)
        elif library == 'polars':
            db_url = self.config.create_connectorx_uri()
            return self._execute_with_polars(db_url, sql_query)
        elif library == 'dask':
            db_url = self.config.create_mysql_uri("pymysql")
            return self._execute_with_dask(db_url, sql_query)
        else:
            raise ValueError(f"Unsupported library: {library}")

    def _execute_with_pandas(self, db_url, sql_query):
        """
        Execute a SQL query using pandas.

        Args:
            db_url (str): Database URL.
            sql_query (str): SQL query to execute.

        Returns:
            pandas.DataFrame: Result of the query.
        """
        pd = importlib.import_module("pandas")
        from sqlalchemy import create_engine, text

        try:
            engine = create_engine(db_url)
            sql_query = text(sql_query)
            if self.verbose:
                print(f"Executing SQL with pandas: {sql_query}")
            with engine.connect() as connection:
                df = pd.read_sql_query(sql_query, connection)
            return df
        except Exception as e:
            print(f"An error occurred while executing the query with pandas: {e}") if self.verbose else None
            return None

    def _execute_with_polars(self, db_url, sql_query):
        """
        Execute a SQL query using polars.

        Args:
            db_url (str): Database URL.
            sql_query (str): SQL query to execute.

        Returns:
            polars.DataFrame: Result of the query.
        """
        pl = importlib.import_module("polars")
        cx = importlib.import_module("connectorx")

        try:
            if self.verbose:
                print(f"Executing SQL with polars: {sql_query}")
            df = cx.read_sql(db_url, sql_query, return_type="polars")
            return df
        except Exception as e:
            print(f"An error occurred while executing the query with polars: {e}") if self.verbose else None
            return None

    def _execute_with_dask(self, db_url, sql_query):
        """
        Execute a SQL query using dask.

        Args:
            db_url (str): Database URL.
            sql_query (str): SQL query to execute.

        Returns:
            dask.DataFrame: Result of the query.
        """
        dd = importlib.import_module("dask.dataframe")
        pd = importlib.import_module("pandas")
        from sqlalchemy import create_engine, text

        try:
            engine = create_engine(db_url)
            sql_query = text(sql_query)
            if self.verbose:
                print(f"Executing SQL with dask: {sql_query}")
            with engine.connect() as connection:
                df = pd.read_sql_query(sql_query, connection)
            dask_df = dd.from_pandas(df, npartitions=4)
            return dask_df
        except Exception as e:
            print(f"An error occurred while executing the query with dask: {e}") if self.verbose else None
            return None

    def cvw(self, view_name, sql):
        """
        Create or replace a view in the database.

        Args:
            view_name (str): Name of the view to create or replace.
            sql (str): SQL query or file name containing the query to define the view.

        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        select_query = self._get_query(sql)
        if select_query is None:
            print(f"Failed to read the SQL file or invalid query: {sql}") if self.verbose else None
            return False

        create_view_sql = f"CREATE OR REPLACE VIEW {view_name} AS {select_query}"
        if self.verbose:
            print(f"Creating view with SQL: {create_view_sql}")
        success = self.config.execute_sql(create_view_sql, use_sqlalchemy=False)
        if success:
            print(f"View '{view_name}' created successfully.") if self.verbose else None
        else:
            print(f"Failed to create view '{view_name}'.") if self.verbose else None
        return success

    def dvw(self, view_name):
        """
        Drop a view from the database.

        Args:
            view_name (str): Name of the view to drop.

        Returns:
            bool: True if the view was dropped successfully, False otherwise.
        """
        drop_view_sql = f"DROP VIEW IF EXISTS {view_name}"
        if self.verbose:
            print(f"Dropping view with SQL: {drop_view_sql}")
        success = self.config.execute_sql(drop_view_sql, use_sqlalchemy=False)
        if success:
            print(f"View '{view_name}' deleted successfully.") if self.verbose else None
        else:
            print(f"Failed to delete view '{view_name}'.") if self.verbose else None
        return success

    def _get_query(self, sql):
        """
        Get the SQL query from a file or direct input.

        Args:
            sql (str): SQL query or file name containing the query.

        Returns:
            str: SQL query.
        """
        if self.config and self.config.sql_dir and (self.config.sql_dir / sql).is_file():
            query = self.config.read_sql_file(sql)
            if self.verbose:
                print(f"Using SQL from file: {query}")
            return query
        if self.verbose:
            print(f"Using direct SQL: {sql}")
        return sql if isinstance(sql, str) else None
