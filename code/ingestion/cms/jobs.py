import dagster as dg

cms_refresh = dg.define_asset_job(
    name = "cms_refresh",
    selection='group:"INGESTION" and kind:"CMS"'
)