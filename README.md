# SQU

The `squ` (**sq**l **u**tilities) module is designed to facilitate interaction with MySQL databases using Pandas, Polars, and Dask dataframes. The module is structured to separate common functionalities and those specific to each library.

## Structure

```
squ/
├── setup.py
├── __init__.py
├── config.py
└── squ.py
```
- `setup.py`: Configuration file for installing the module and its optional dependencies.
- `__init__.py`: Initializes the module and makes functionalities available for import.
- `config.py`: Contains common methods and utilities, including reading SQL files and database connection configuration.
- `squ.py`: Provides methods and utilities to interact with MySQL databases and store results in Pandas, Polars, and Dask dataframes.

## Public methods

- `qpd` (query to pandas dataframe)
- `qpl` (query to polars dataframe)
- `qdd` (query to dask dataframe)

These methods read environment variables from a `.env` file in the project's root directory, execute an SQL query from a specified file, and return a Pandas, Polars, or Dask DataFrame as appropriate.

## Installing Dependencies

The `SQU` module allows you to install optional dependencies according to the library you want to use. Make sure you are in the project directory where `setup.py` is located to install the dependencies.

To install Pandas dependencies:

```bash
pip install 'squ[pandas] @ git+https://github.com/Spot2HQ/squ.git'

```
To install Polars dependencies:

```bash
pip install 'squ[polars] @ git+https://github.com/Spot2HQ/squ.git'
```
To install Dask dependencies:

```bash
pip install 'squ[dask] @ git+https://github.com/Spot2HQ/squ.git'
```
To install multiple optional dependencies:

```bash
pip install 'squ[pandas,polars] @ git+https://github.com/Spot2HQ/squ.git'
```

## Usage

Example with Pandas:
```python
from squ import SQU

# Initialization with directories for SQL and .env
su = SQU("/path/to/sql/dir", "/path/to/.env")

# Execute a query and store the result in a Pandas DataFrame
df_pandas = su.qpd("query.sql")

# Display the DataFrame
print(df_pandas)

```

Example with Polars:

```python
from squ import SQU

# Initialization with directories for SQL and .env
su = SQU("/path/to/sql/dir", "/path/to/.env")

# Execute a query and store the result in a Polars DataFrame
df_polars = su.qpl("query.sql")

# Display the DataFrame
print(df_polars)

```

Example with Dask:

```python
from squ import SQU

# Initialization with directories for SQL and .env
su = SQU("/path/to/sql/dir", "/path/to/.env")

# Execute a query and store the result in a Dask DataFrame
df_dask = su.qdd("query.sql")

# Display the DataFrame
print(df_dask)

```

## Important Note

Support for Dask is currently preliminary and relies on Pandas as an intermediary for establishing the connection. Ensure you have the Pandas dependencies installed when using Dask.

## Summary

This module allows you to easily switch between using Pandas, Polars, and Dask for data manipulation and analysis while working with MySQL databases.
