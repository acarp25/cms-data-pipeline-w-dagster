import requests
import datetime as dt

import dagster as dg

from . import jobs
from . import assets

@dg.sensor(
    job=jobs.cms_refresh,
    minimum_interval_seconds=60*60, # check every hour
    description="Sensor to check for new monthly Medicare Advantage Enrollment data",
)
def medicare_advantage_enrollment_by_state_county_contract_sensor(context: dg.SensorEvaluationContext):
    if context.cursor:
        context.log.info(f"Resuming from cursor: {context.cursor}")
        previous_month_year = context.cursor
        # parse the previous cursor (e.g. "November-2025") then compute next month
        prev_dt = dt.datetime.strptime(previous_month_year, "%B-%Y")
        # advance by exactly one month
        year = prev_dt.year + (prev_dt.month // 12)
        month = prev_dt.month % 12 + 1
        next_dt = dt.datetime(year, month, 1)
        month_year = next_dt.strftime("%B-%Y")
    else:
        # Start with January 2024
        context.log.info("No cursor found, starting from January 2024")
        month_year = dt.datetime(2024, 1, 1).strftime("%B-%Y")
    
    #month-year -> november-2025
    context.log.info(f"Checking data availability for {month_year}")
    base_url = f"https://www.cms.gov/files/zip/ma-enrollment-state-county-contract-{month_year}-abridged-version-exclude-rows-10-or-less-enrollees.zip"
    backup_url = f"https://www.cms.gov/files/zip/ma-enrollment-state/county/contract-{month_year}-abridged-version-exclude-rows-10-or-less-enrollees.zip"
    backup_url_2 = f"https://www.cms.gov/files/zip/ma-enrollment-state/county/contract-{month_year}-abridged-version-exclude-rows-10-or-less.zip"
    
    try:
        urls = [base_url, backup_url, backup_url_2]
        for url in urls:
            try:
                response = requests.head(url, allow_redirects=True, timeout=10)
            except requests.RequestException as e:
                context.log.warning(f"Error checking URL {url}: {e}")
                # try the next URL
                continue

            if response.status_code == 200:
                context.log.info(f"Data source is available at {url}.")
                context.update_cursor(month_year)
                return dg.SensorResult(
                    run_requests=[
                        dg.RunRequest(
                            run_key=f"{month_year}",
                            partition_key=month_year,
                            run_config={
                                "ops": {
                                    "medicare_advantage_enrollment_by_state_county_contract": {
                                        "config": {"url": url}
                                    }
                                }
                            },
                        )
                    ],
                    dynamic_partitions_requests=[assets.cms_monthly_partitions.build_add_request([month_year])],
                )
            else:
                context.log.info(f"URL {url} returned status code {response.status_code}; trying next URL if available.")

        # If we get here, none of the URLs returned 200
        context.log.warning(f"Data source not found at any tested URL for {month_year}. No run will be triggered.")
        return dg.SkipReason(f"Data for {month_year} not yet available.")
    except requests.RequestException as e:
        context.log.error(f"Error checking data source availability: {e}")
        return dg.SkipReason(f"Error checking data source availability: {e}")