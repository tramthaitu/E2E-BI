from dagster import ScheduleDefinition
from Dagster.jobs.job import etl_job

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 8 * * *"  # chạy lúc 8h sáng hàng ngày
)
