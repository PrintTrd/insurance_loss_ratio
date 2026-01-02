from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import ingestion
from .assets.dbt_assets import medical_cost_dbt_assets, dbt_project

ingestion_assets = load_assets_from_modules([ingestion])

defs = Definitions(
    assets=[*ingestion_assets, medical_cost_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)