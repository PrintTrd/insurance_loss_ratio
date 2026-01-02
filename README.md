Installation (Python 3.11.9)
python -m venv venv
python -m pip install --upgrade pip
pip install "setuptools<70"
pip install dbt-core dbt-postgres dagster dagster-webserver dagster-dbt dagster-postgres pandas duckdb faker psycopg2-binary

netstat -ano | findstr :5432

.\set_env.ps1
.\venv\Scripts\activate
docker-compose up -d
dagster dev -m medical_cost_etl.definitions