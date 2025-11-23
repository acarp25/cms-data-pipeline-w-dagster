import dagster as dg
from dagster_duckdb import DuckDBResource

from .ingestion.cms import assets as cms_assets, jobs as cms_jobs, sensors as cms_sensors

all_assets = dg.load_assets_from_modules([cms_assets])

defs = dg.Definitions(
    assets=all_assets,
    jobs=[cms_jobs.cms_refresh],
    # schedules=[every_weekday_9am],
    sensors=[cms_sensors.medicare_advantage_enrollment_by_state_county_contract_sensor],
    resources={
        "duckdb": DuckDBResource(
            database="data/duckdb/cms_data.duckdb",
        ),
    }
)