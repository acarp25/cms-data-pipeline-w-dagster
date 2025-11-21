from pathlib import Path

import dagster as dg 
from dagster_duckdb import DuckDBResource


#monthly medicare advantage enrollment by state/county/contract
#https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract

#set db and schema based on folder structure, (ingestion, cms)
DATABASE, SCHEMA = Path(__file__).parent.parent.name, Path(__file__).parent.name



@dg.asset(
    group_name="cms",
)
def medicare_advantage_enrollment_by_state_county_contract(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    context.log.info(f"Loading data into {DATABASE}.{SCHEMA}.medicare_advantage_enrollment_by_state_county_contract")
    pass