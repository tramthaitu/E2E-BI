from dagster import Definitions
from Dagster.assets.lailo import dim_to_staging, fact_to_staging, etl_to_dwh
from Dagster.jobs.job import etl_job
from Dagster.schedules.schedules import etl_schedule

defs = Definitions(
    assets=[dim_to_staging, fact_to_staging, etl_to_dwh],
    jobs=[etl_job],
    schedules=[etl_schedule],
)
