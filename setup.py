from setuptools import setup, find_packages

setup(
    name="squ",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
    ],
    extras_require={
        "pandas": ["pandas", "sqlalchemy", "pymysql"],
        "polars": ["polars", "connectorx", "pyarrow"],
        "dask": ["dask[dataframe]", "pandas", "sqlalchemy", "pymysql"],
    },
    url="https://github.com/Spot2HQ/squ.git",
    author="Luis Muñiz Valledor",
    author_email="luis.muniz@spot2.mx",
    description="The squ (sql utilities) module is designed to facilitate interaction with MySQL databases using Pandas, Polars, and Dask dataframes.",
)
