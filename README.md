# cms-data-pipeline-w-dagster

A small example data pipeline demonstrating Dagster-based ingestion of CMS data into DuckDB.

## Highlights

- **Dagster pipelines:** example assets, jobs, and sensors under `code/ingestion/cms`.
- **Data storage:** usage of DuckDB for local, fast analytics (`data/duckdb/cms_data.duckdb`).
- **Organized code:** reusable utilities in `code/utils` (DuckDB helpers, etc.).

## Quickstart — install dependencies with `uv`

Recommended steps to get a local dev environment running using the `uv` package manager. Running `uv sync` will create a project-local virtual environment at `.venv` and install the dependencies listed in `pyproject.toml`.

```bash
uv sync
source .venv/bin/activate
```

## Usage

- Use Dagster local tooling to run or test assets (i.e., `dagster dev`).
- Dagster project's entrypoint is `code/definitions.py`

## Orchestration

- Utilizing [partitions](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract) in dagster to easily track each month's data set
- Sensor logic accomodates different URL formats from CMS and checks for the next month's available file
    - when that file is available, we trigger a job to materialize the asset and pass the appropriate URL to the run

## Data

- Using CMS data for [Monthly MA Enrollment by State/County/Contract](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract)
    - Available monthly with minimal exceptions (e.g. government shutdowns)
- Local DuckDB database: `data/duckdb/cms_data.duckdb`.
    - Can query using dbeaver.
    - enables fast, local deployment with database in a single file