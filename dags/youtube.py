'''
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
'''

import os
import pickle
import pandas as pd
import logging
from dotenv import load_dotenv

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery

# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from airflow.models.baseoperator import chain


default_args = {
    'owner': 'youtube_client'
}

load_dotenv()

videos = {}

def get_new_credentials():
    client_secrets_path = os.getenv('CLIENT_SECRETS_PATH')
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, scopes=["https://www.googleapis.com/auth/youtube.readonly"])
    credentials = flow.run_local_server(port=4040, authorization_prompt_message='')

    return credentials


def get_valid_credentials():
    credentials = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
            logging.info("Credentials were pulled from a token.pickle")

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token: # credentials are expired
            credentials.refresh(Request())
            logging.info("Credentials were refreshed")

        else: # credentials not exist
            credentials = get_new_credentials()
            logging.info("New credentials were created")
            
            # write new credentials to token.pickle
            with open('token.pickle', 'wb') as f:
                pickle.dump(credentials, f)

    return credentials


def extract_my_playlists(youtube) -> dict:
    
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        maxResults=50,
        mine=True
    )
    response = request.execute()

    playlists = {}
    
    for item in response['items']:
        if '💼' not in item['snippet']['title']: # remove non-music playlists
            playlists[item['id']] = item['snippet']['title']
    
    logging.info(f"{len(playlists)} playlists were extracted")
    return playlists


def extract_playlist_videos(youtube, playlist_id: str, nextPageToken: str = ''):
    if nextPageToken:
        request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=playlist_id,
                pageToken=nextPageToken
            )
    else:
        request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                maxResults=50,
                playlistId=playlist_id
            )
    
    response = request.execute()
    return response


def populate_videos(response):
    for item in response['items']:
        # Remove deleted, private and duplicate videos
        if item['snippet']['title'] not in ('Deleted video', 'Private video') and item['contentDetails']['videoId'] not in videos:
            videos[item['contentDetails']['videoId']] = (item['snippet']['title'], item['snippet']['videoOwnerChannelTitle'])


def extract_all_videos(youtube, playlists: dict):
    for playlist_id in playlists:
    
        response = extract_playlist_videos(youtube, playlist_id)
        populate_videos(response)

        while response.get('nextPageToken', ''):
            response = extract_playlist_videos(youtube, playlist_id, response['nextPageToken'])
            populate_videos(response)


def transform_playlists_to_df(playlists: dict) -> pd.DataFrame:
    playlists_series = pd.Series(playlists)
    df_playlists = pd.DataFrame(playlists_series, columns=['playlist_name'])
    return df_playlists


def transform_videos_to_df(videos: dict) -> pd.DataFrame:
    df_videos = pd.DataFrame(videos).T
    df_videos = df_videos.rename(columns={0: 'title', 1: 'channel_name'})
    return df_videos


def load_to_bigquery(df: pd.DataFrame, table_name: str) -> None:
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)
    table_id = f'{project_id}.marts.{table_name}'

    # Create a table in the marts dataset and populate it with data from the dataframe
    client.load_table_from_dataframe(df, table_id).result()
    logging.info(f"Dataframe was succecfully loaded to a BigQuery table: {table_name}")


if __name__ == '__main__':
    credentials = get_valid_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    playlists = extract_my_playlists(youtube)
    df_playlists = transform_playlists_to_df(playlists)
    load_to_bigquery(df_playlists, 'playlists')

    extract_all_videos(youtube, playlists)
    df_videos = transform_videos_to_df(videos)
    load_to_bigquery(df_videos, 'videos')



# with DAG(
#     dag_id='extract_my_youtube_videos',
#     default_args=default_args,
#     description='Youtube DAG',
#     start_date=datetime(2023, 8, 11),
#     schedule='@hourly', # None
#     catchup=False,
# ) as dag:
    
#     connect_to_account = PythonOperator(
#         task_id = 'connect_to_account',
#         python_callable=connect
#     )

#     extract_my_playlists = PythonOperator(
#         task_id = 'extract_my_playlists',
#         python_callable=extract_my_playlists
#     )

#     extract_all_videos = PythonOperator(
#         task_id = 'extract_all_videos',
#         python_callable=extract_all_videos
#     )


#     chain(
#         connect_to_account,
#         extract_my_playlists,
#         extract_all_videos
#     )