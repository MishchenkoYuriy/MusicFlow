import os
import pandas as pd
from dotenv import load_dotenv

from google.cloud import bigquery

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable


# default_args = {
#     'owner': 'spotify_client'
# }

load_dotenv()


def extract_videos() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)
    table_id = f'{project_id}.marts.videos'

    sql = f"""
    SELECT channel_name, title
    FROM `{table_id}`
    ORDER BY channel_name, title
    """

    df_videos = client.query(sql).to_dataframe()
    return df_videos


def get_authorization_code():
    scope = ["user-library-modify"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    return sp


def find_spotify_id(row) -> str:
    '''
    Priority: album -> playlist -> artist -> track
    '''
    results = sp.search(q=row['title'], limit=1, type='track')
    item_id = results['tracks']['items'][0]['id']
    return item_id


def add_to_saved_tracks(track_id: str) -> None:
    sp.current_user_saved_tracks_add(tracks=[track_id])


def load_to_spotify(row) -> None:
    item_id = find_spotify_id(row)
    add_to_saved_tracks(item_id)



if __name__ == '__main__':
    df_videos = extract_videos()

    sp = get_authorization_code()
    df = df_videos[0:5]
    df.apply(load_to_spotify, axis = 1)



# with DAG(
#     dag_id='request_access_token',
#     default_args=default_args,
#     description='Spotify DAG',
#     start_date=datetime(2023, 8, 11),
#     schedule='@hourly', # None
#     catchup=False,
# ) as dag:
    
#     request_access_token_task = PythonOperator(
#         task_id = 'request_access_token_task',
#         python_callable=request_access_token
#     )
    
    
#     request_access_token_task
