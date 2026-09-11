from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

#Define local timezone
local_tz = pendulum.timezone("America/New_York")

#Default Args
default_args = {
    'owner': "dataengineers",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': "data@engineers.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

#Variables

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to produce a JSON file with raw data',
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    # Define Tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    #define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into bioth staging and core schemas",
    schedule="0 16 * * *",
    catchup=False,
) as dag:

    # Define Tasks
    update_staging = staging_table()
    update_core = core_table()
    
    #define dependencies
    update_staging >> update_core

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core