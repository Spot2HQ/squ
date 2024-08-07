from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="squ",
    version="0.2",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
    ],
    extras_require={
        "pandas": ["pandas", "sqlalchemy", "pymysql", "cryptography"],
        "polars": ["polars", "connectorx", "pyarrow"],
        "dask": ["dask[dataframe,diagnostics]", "pandas", "sqlalchemy", "pymysql", "cryptography"],
        "view": ["sqlalchemy", "pymysql"],
        "tests": ["pytest", "pytest-mock", "tox"],
        "all": [
            "pandas", "sqlalchemy", "pymysql", "polars", "connectorx", "pyarrow",
            "dask[dataframe,diagnostics]", "pytest", "pytest-mock", "tox", "cryptography"
        ]
    },
    url="https://github.com/Spot2HQ/squ.git",
    author="Luis Muñiz Valledor",
    author_email="luis.muniz@spot2.mx",
    description="The squ (sql utilities) module is designed to facilitate interaction with MySQL databases using Pandas, Polars, and Dask dataframes.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
)
