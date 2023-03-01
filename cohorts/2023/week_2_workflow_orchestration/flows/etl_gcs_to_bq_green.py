from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credentials")

    df.to_gbq(
        destination_table="nytaxi.green_tripdata",
        project_id="cosmic-stacker-376320",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    print(f'rows: {len(df)}')


@flow()
def etl_parent_flow_1(months: list[int] = [1, 2],
                    years: list[int] = [2019, 2020],
                    color: str = "yellow"
):
    for year in years:
        for month in months:
            etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "green"
    months = list(range(1, 13))
    year = [2019, 2020]
    etl_parent_flow_1(months, year, color)
    color = "green"
    months = list(range(1, 8))
    year = [2021]
    etl_parent_flow_1(months, year, color)
