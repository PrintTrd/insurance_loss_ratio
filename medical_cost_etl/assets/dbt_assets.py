from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
from pathlib import Path

CURRENT_FILE_PATH = Path(__file__).resolve()
PROJECT_ROOT_DIR = CURRENT_FILE_PATH.parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT_DIR / "medical_cost_dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def medical_cost_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
