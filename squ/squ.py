import importlib
from .config import Config

class SQU:
    def __init__(self, sql_dir, env_path):
        self.config = Config(sql_dir, env_path)

    def qpd(self, file_name):
        return self._execute_query(file_name, 'pandas')

    def qpl(self, file_name):
        return self._execute_query(file_name, 'polars')

    def qdd(self, file_name):
        return self._execute_query(file_name, 'dask')

    def _execute_query(self, file_name, library):
        sql_query = self.config.read_sql_file(file_name)
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
        pd = importlib.import_module("pandas")
        from sqlalchemy import create_engine, text

        try:
            engine = create_engine(db_url)
            sql_query = text(sql_query)
            with engine.connect() as connection:
                df = pd.read_sql_query(sql_query, connection)
            return df
        except Exception as e:
            print(f"An error occurred while executing the query with pandas: {e}")
            return None

    def _execute_with_polars(self, db_url, sql_query):
        pl = importlib.import_module("polars")
        cx = importlib.import_module("connectorx")

        try:
            df = cx.read_sql(db_url, sql_query, return_type="polars")
            return df
        except Exception as e:
            print(f"An error occurred while executing the query with polars: {e}")
            return None

    def _execute_with_dask(self, db_url, sql_query):
        dd = importlib.import_module("dask.dataframe")
        pd = importlib.import_module("pandas")
        from sqlalchemy import create_engine, text

        try:
            engine = create_engine(db_url)
            sql_query = text(sql_query)
            with engine.connect() as connection:
                df = pd.read_sql_query(sql_query, connection)
            dask_df = dd.from_pandas(df, npartitions=4)
            return dask_df
        except Exception as e:
            print(f"An error occurred while executing the query with dask: {e}")
            return None
