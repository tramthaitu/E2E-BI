from dagster import define_asset_job
from Dagster.assets.lailo import dim_to_staging, fact_to_staging, etl_to_dwh

etl_job = define_asset_job(
    "etl_job",
    selection=[dim_to_staging, fact_to_staging, etl_to_dwh]
)
