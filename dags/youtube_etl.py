'''
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
'''

import os
import pickle
import aniso8601
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


# default_args = {
#     'owner': 'youtube_client'
# }

load_dotenv()

playlist_items: dict[str, list[str]] = {}
liked_videos: dict[str, list[str]] = {}


def get_new_credentials():
    client_secrets_path = os.getenv('CLIENT_SECRETS_PATH')
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path,
                                                     scopes=["https://www.googleapis.com/auth/youtube.readonly"])
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


def extract_my_playlists(youtube) -> dict[str, str]:
    """
    Return a dictionary object that contains playlist ids and titles
    of all music playlists created by current user.
    """
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        maxResults=50,
        mine=True
    )
    response = request.execute()

    playlists: dict[str, str] = {}
    
    for item in response['items']:
        if '💼' not in item['snippet']['title']: # remove non-music playlists
            playlists[item['id']] = item['snippet']['title']
    
    logging.info(f"{len(playlists)} playlists were extracted")
    return playlists


def extract_playlist_videos(youtube, playlist_id: str, nextPageToken: str = ''):
    """
    Return a requested page of the API call as a json object.
    Contains information about videos in the playlist.
    """
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


def populate_videos(response, playlist_id: str) -> None:
    for item in response['items']:
        # Remove deleted, private and duplicate videos
        if item['snippet']['title'] not in ('Deleted video', 'Private video') and item['contentDetails']['videoId'] not in playlist_items:
            playlist_items[item['contentDetails']['videoId']] = [playlist_id,
                                                                 item['snippet']['title'],
                                                                 item['snippet']['videoOwnerChannelTitle'],
                                                                 item['snippet']['description']]


def extract_all_playlist_items(youtube, playlists: dict[str, str]) -> None:
    """
    Extract playlist items info (video ids, titles and channel names)
    and store it in the playlist_items dictionary.

    Args:
        playlists: iterable object that contains playlist ids
    """
    for playlist_id in playlists:
    
        response = extract_playlist_videos(youtube, playlist_id)
        populate_videos(response, playlist_id)

        while response.get('nextPageToken', ''):
            response = extract_playlist_videos(youtube, playlist_id, response['nextPageToken'])
            populate_videos(response, playlist_id)


def add_ms_duration(youtube) -> None:
    """
    Split video ids into 50-size chunks, call videos().list for each chunk.
    Extract video duration, convert it to milliseconds and store it in the playlist_items dictionary.
    """
    chunks: list[list[str]] = []
    chunk: list[str] = []

    for ind, video in enumerate(playlist_items):
        if ind % 50 == 0 and chunk: # not include the first empty chunk
            chunks.append(chunk)
            logging.info(f'Chunk with the length of {len(chunk)} was added, total chunks added: {len(chunks)}')
            chunk = []
        chunk.append(video)

    chunks.append(chunk) # append the last chunk
    logging.info(f'Chunk with the length of {len(chunk)} was added, total chunks added: {len(chunks)}')
    
    str_chunks: list[str] = [','.join(chunk) for chunk in chunks]

    for chunk_ind, str_chunk in enumerate(str_chunks):
        request = youtube.videos().list(part="snippet,contentDetails", id=str_chunk)
        response = request.execute()
        
        for item_ind, item in enumerate(response['items']):
            video_id = chunks[chunk_ind][item_ind] # get the videoId
            
            iso8601_duration = item['contentDetails']['duration']
            duration_ms = int(aniso8601.parse_duration(iso8601_duration).total_seconds()*1000)
            playlist_items[video_id].append(duration_ms)


def extract_liked_videos(youtube, nextPageToken: str = ''):
    '''
    Return a requested page of the API call as a json object.
    Contains information about liked videos.
    '''
    if nextPageToken:
        request = youtube.videos().list(
                part="snippet,contentDetails",
                maxResults=50,
                myRating='like',
                pageToken=nextPageToken
            )
    else:
        request = youtube.videos().list(
                part="snippet,contentDetails",
                maxResults=50,
                myRating='like'
            )
        
    response = request.execute()
    return response


def populate_liked_videos(response):
    # deleted and private videos actomaticly excluded by YouTube Data API
    for item in response['items']:
        iso8601_duration = item['contentDetails']['duration']
        duration_ms = int(aniso8601.parse_duration(iso8601_duration).total_seconds()*1000)
        liked_videos[item['id']] = [item['snippet']['title'],
                                    item['snippet']['channelTitle'],
                                    item['snippet']['description'],
                                    duration_ms]


def extract_all_liked_videos(youtube) -> None:
    """
    Extract liked videos info (video ids, titles, channel names and duration)
    and store it in the liked_videos dictionary.
    """
    response = extract_liked_videos(youtube)
    populate_liked_videos(response)

    while response.get('nextPageToken', ''):
        response = extract_liked_videos(youtube, response['nextPageToken'])
        populate_liked_videos(response)


def playlists_to_df(playlists: dict) -> pd.DataFrame:
    """
    Return a playlists dataframe from a playlist dictionary.
    """
    playlists_series = pd.Series(playlists)
    df_playlists = pd.DataFrame(playlists_series, 
                                columns=['playlist_name']) \
                               .reset_index(names='youtube_playlist_id')
    return df_playlists


def playlist_items_to_df() -> pd.DataFrame:
    """
    Return a playlist_items dataframe from a playlist_items dictionary.
    """
    df_playlist_items = pd.DataFrame.from_dict(playlist_items, orient='index',
                                               columns=['youtube_playlist_id',
                                                        'title',
                                                        'channel_name',
                                                        'description',
                                                        'duration_ms']) \
                                              .reset_index(names='video_id')
    return df_playlist_items


def liked_videos_to_df() -> pd.DataFrame:
    """
    Return a liked_videos dataframe from a liked_videos dictionary.
    """
    df_liked_videos = pd.DataFrame.from_dict(liked_videos, orient='index',
                                             columns=['title',
                                                      'channel_name',
                                                      'description',
                                                      'duration_ms']) \
                                            .reset_index(names='video_id')
    return df_liked_videos


def load_to_bigquery(df: pd.DataFrame, table_name: str, method: str) -> None:
    """
    Upload the dataframe in Google BigQuery.
    Create, replace or append depending on the method passed.
    """
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)
    table_id = f'{project_id}.marts.{table_name}'

    if method == 'replace':
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    elif method == 'append':
        client.load_table_from_dataframe(df, table_id).result()

    else:
        raise Exception('Unexpected method')


if __name__ == '__main__':
    credentials = get_valid_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    playlists = extract_my_playlists(youtube)
    df_playlists = playlists_to_df(playlists)
    load_to_bigquery(df_playlists, 'youtube_playlists', 'replace')
    print(f'youtube_playlists uploaded to BigQuery, {len(df_playlists)} rows.')

    extract_all_playlist_items(youtube, playlists)
    add_ms_duration(youtube)
    df_playlist_items = playlist_items_to_df()
    load_to_bigquery(df_playlist_items, 'youtube_videos', 'replace')
    print(f'playlist_items uploaded to BigQuery, {len(df_playlist_items)} rows.')

    extract_all_liked_videos(youtube)
    df_liked_videos = liked_videos_to_df()
    load_to_bigquery(df_liked_videos, 'youtube_videos', 'append')
    print(f'liked_videos uploaded to BigQuery, {len(df_liked_videos)} rows.')




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