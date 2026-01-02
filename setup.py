from setuptools import find_packages, setup

setup(
    name="insurance_loss_ratio",
    version="0.0.1",
    packages=find_packages(exclude=["insurance_loss_ratio_tests"]),
    
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core",
        "dbt-postgres",
        "pandas",
        "psycopg2-binary", # driver for postgres
        "duckdb",
        "faker",           # for mock pii
        "python-dotenv",   # load .env
        "sqlalchemy",
    ],

    # Library for Development Environment
    extras_require={
        "dev": [
            "dagster-webserver", 
            "pytest",
        ]
    },
)
