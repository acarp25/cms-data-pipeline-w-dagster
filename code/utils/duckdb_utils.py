from pathlib import Path
import tempfile, requests, zipfile
import pandas as pd

import dagster as dg 
from dagster_duckdb import DuckDBResource

def table_exists(duckdb: DuckDBResource, table_name: str, schema: str) -> bool:
    """
    Check if a table exists in the specified schema of the DuckDB database.

    Args:
        duckdb (DuckDBResource): The DuckDB resource to use for the connection.
        table_name (str): The name of the table to check.
        schema (str): The schema where the table resides.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    full_table_name = f"{schema}.{table_name}"
    with duckdb.get_connection() as conn:
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM duckdb_tables()
            WHERE schema_name = '{schema}' 
            AND table_name = '{table_name}';
        """).fetchone()
    return result[0] > 0

def load_dataframe_to_duckdb(context: dg.AssetExecutionContext, duckdb: DuckDBResource, df: pd.DataFrame, table_name: str, schema: str, overwrite: bool = False):
    """
    Load a pandas DataFrame into a DuckDB table within the specified schema.
    If the table does not exist, it will be created. If it exists, data will be appended.

    Args:
        context (dg.AssetExecutionContext): The Dagster asset execution context.
        duckdb (DuckDBResource): The DuckDB resource to use for the connection.
        df (pd.DataFrame): The DataFrame to load into DuckDB.
        table_name (str): The name of the table to load data into.
        schema (str): The schema where the table resides.
        overwrite (bool): If True, overwrite the existing table. Default is False (append).
    """
    full_table_name = f"{schema}.{table_name}"
    exists = table_exists(duckdb, table_name, schema)
    with duckdb.get_connection() as conn:
        if not exists:
            context.log.info(f"Table {full_table_name} does not exist. Creating table.")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM df;")
            return None

        if not overwrite:
            context.log.info(f"Table {full_table_name} exists. Appending data.")
            conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM df;")
        else:
            context.log.info(f"Overwriting table {full_table_name}.")
            conn.execute(f"CREATE OR REPLACE TABLE {full_table_name} as SELECT * FROM df;")
    return None

def upsert_dataframe_to_duckdb(context: dg.AssetExecutionContext, duckdb: DuckDBResource, df: pd.DataFrame, table_name: str, schema: str, key_columns: list):
    """
    Upsert a pandas DataFrame into a DuckDB table within the specified schema.
    If the table does not exist, it will be created. If it exists, records will be merged based on key columns.

    Args:
        context (dg.AssetExecutionContext): The Dagster asset execution context.
        duckdb (DuckDBResource): The DuckDB resource to use for the connection.
        df (pd.DataFrame): The DataFrame to upsert into DuckDB.
        table_name (str): The name of the table to upsert data into.
        schema (str): The schema where the table resides.
        key_columns (list): List of column names to use as keys for the upsert operation.
    """
    full_table_name = f"{schema}.{table_name}"
    exists = table_exists(duckdb, table_name, schema)
    with duckdb.get_connection() as conn:
        if not exists:
            context.log.info(f"Table {full_table_name} does not exist. Creating table.")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM df;")
            return None

        context.log.info(f"Table {full_table_name} exists. Performing upsert operation.")
        # Create a temporary table to hold new data
        temp_table = f"{table_name}_temp"
        conn.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM df;")

        # Build the merge condition
        key_columns = [f'"{col}"' if ' ' in col else col for col in key_columns ]
        merge_condition = " AND ".join([f'target.{col} = source.{col}' for col in key_columns])

        # Perform the upsert using MERGE statement
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {temp_table} AS source
        ON ({merge_condition})
        WHEN MATCHED THEN UPDATE
        WHEN NOT MATCHED THEN INSERT;
        """
        context.log.info(f"Executing MERGE SQL:\n{merge_sql}")
        conn.execute(merge_sql)
    return None