from pathlib import Path
import tempfile, requests, zipfile

import dagster as dg 
from dagster_duckdb import DuckDBResource

#monthly medicare advantage enrollment by state/county/contract
#https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract

#set db and schema based on folder structure, (ingestion, cms)
DATABASE, SCHEMA = Path(__file__).parent.parent.name, Path(__file__).parent.name

@dg.asset(
    group_name=DATABASE.upper(),
    description="Medicare Advantage Enrollment by State, County, and Contract",
    kinds={"CMS"},
)
def medicare_advantage_enrollment_by_state_county_contract(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    url = "https://www.cms.gov/files/zip/ma-enrollment-state-county-contract-november-2025-abridged-version-exclude-rows-10-or-less-enrollees.zip"
    # Download zip to temp location in data directory
    data_dir = Path("data/temp/")
    data_dir.mkdir(exist_ok=True, parents=True)
    
    context.log.info(f"Downloading data from {url}")
    with tempfile.NamedTemporaryFile(suffix=".zip", dir=data_dir, delete=False) as tmp_zip:
        response = requests.get(url)
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

    context.log.info(f"Loading data into {SCHEMA}.medicare_advantage_enrollment_by_state_county_contract")
    table_name = f"medicare_advantage_enrollment_by_state_county_contract"
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name} AS SELECT * FROM read_csv_auto('{extracted_csv_path}')")
        num_rows = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table_name}").fetchone()[0]

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
            "num_rows": dg.MetadataValue.int(num_rows)
        }
    )

@dg.asset()
def dummy_asset():
    pass