from pathlib import Path
import tempfile, requests, zipfile
import pandas as pd

import dagster as dg 
from dagster_duckdb import DuckDBResource

from ...utils import duckdb_utils as db_utils

#monthly medicare advantage enrollment by state/county/contract
#https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract

#set db and schema based on folder structure, (ingestion, cms)
DATABASE, SCHEMA = Path(__file__).parent.parent.name, Path(__file__).parent.name

# using dynamic partitions over fixed monthly partitions
# it's often the case that the new month of data is added irregularly 
# for the case of the 2025 gov shutdown, data was delayed 2 months
cms_monthly_partitions = dg.DynamicPartitionsDefinition(name="cms_monthly_partitions")

class cms_config(dg.Config):
    url: str = "https://www.cms.gov/files/zip/ma-enrollment-state-county-contract-{partition_key}-abridged-version-exclude-rows-10-or-less-enrollees.zip"

@dg.asset(
    group_name=DATABASE.upper(),
    description="Medicare Advantage Enrollment by State, County, and Contract",
    kinds={"CMS"},
    partitions_def=cms_monthly_partitions,
)
def medicare_advantage_enrollment_by_state_county_contract(context: dg.AssetExecutionContext, duckdb: DuckDBResource, config: cms_config):
    partition_key = context.partition_key
    url = config.url.format(partition_key=partition_key)
    # Download zip to temp location in data directory
    data_dir = Path("data/temp/")
    data_dir.mkdir(exist_ok=True, parents=True)
    
    context.log.info(f"Downloading data from {url}")
    with tempfile.NamedTemporaryFile(suffix=".zip", dir=data_dir, delete=False) as tmp_zip:
        response = requests.get(url)
        if response.status_code != 200:
            context.log.error(f"Failed to download data: HTTP {response.status_code}")
            raise ValueError(f"Failed to download data: HTTP {response.status_code}")
        tmp_zip.write(response.content)
        zip_path = tmp_zip.name

    context.log.info(f"Extracting CSV from {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        csv_names = [f for f in zip_ref.namelist() if f.endswith(".csv")]
        if not csv_names:
            raise ValueError("No CSV file found in ZIP")
        csv_name = csv_names[0]
        extracted_csv_path = data_dir / csv_name
        zip_ref.extract(csv_name, path=data_dir)
    
    #add partition_key to column
    df = pd.read_csv(extracted_csv_path)
    df["partition_key"] = partition_key
    df["record_created_at"] = pd.Timestamp.now()
    num_rows = len(df) 

    context.log.info(f"Loading data into {SCHEMA}.medicare_advantage_enrollment_by_state_county_contract")
    table_name = f"medicare_advantage_enrollment_by_state_county_contract"
    db_utils.load_dataframe_to_duckdb(context, duckdb, df, table_name, SCHEMA, overwrite=False)        

    context.log.info(f"Cleaning up temporary files")
    for item in data_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            import shutil
            shutil.rmtree(item)
    data_dir.rmdir()

    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text(f"{SCHEMA}.{table_name}"),
            "url": dg.MetadataValue.text(url),
            "num_rows": dg.MetadataValue.int(num_rows)
        }
    )

@dg.asset()
def dummy_asset():
    pass