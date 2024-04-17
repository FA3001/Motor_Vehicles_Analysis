from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'magic'  # Specify the name of the schema to export data to
    # table_name = 'MV_db'  # Specify the name of the table to export data to
    years_to_export = [2020, 2021, 2022]


    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Export Vehicles
        # Specify the years you want to export
        for year in years_to_export:
            df_year = df[pd.to_datetime(df['crash_date']).dt.year == year]
            if not df_year.empty:
                table_name = f"MVC_C_{year}"
                loader.export(
                    df_year,
                    schema_name,
                    table_name,
                    index=False,
                    if_exists='replace',
                )
            else:
                print(f"No data for year {year}, skipping export.")
#metabase3001 metabase3001