from setuptools import find_packages, setup

setup(
    name="insurance_loss_ratio",
    version="0.0.1",
    packages=find_packages(exclude=["medical_cost_dbt", "tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-postgres",
        "dagster-webserver",
        "dbt-core",
        "dbt-postgres",
        "pandas",
        "duckdb",
        "faker",  # for mock pii
        "psycopg2-binary",  # driver for postgres
        "python-dotenv",  # load .env
        "setuptools<70",
        "sqlalchemy",
    ],
    # Library for Development Environment
    extras_require={
        "dev": [
            "pytest",
        ]
    },
)
