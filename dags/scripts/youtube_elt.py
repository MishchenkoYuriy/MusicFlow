'''
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
'''

import os
import pickle
import aniso8601
import pandas as pd
from dotenv import load_dotenv

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.auth.exceptions import RefreshError


load_dotenv()


def get_and_write_new_credentials():
    client_secrets_path = os.getenv('CLIENT_SECRETS_PATH')
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path,
                                                     scopes=["https://www.googleapis.com/auth/youtube.readonly"])
    credentials = flow.run_local_server(port=4040, authorization_prompt_message='')

    # write new credentials to token.pickle
    with open('token.pickle', 'wb') as f:
        pickle.dump(credentials, f)

    return credentials


def get_valid_credentials():
    credentials = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    
    if not credentials: # credentials not exist
        credentials = get_and_write_new_credentials()
        print("A refresh token has been created.")
    
    elif not credentials.valid:
        try:
            credentials.refresh(Request())

        except RefreshError:
            os.unlink('token.pickle') # delete token.pickle
            print("The refresh token has been expired after 7 days. Please reauthorise...")
            credentials = get_and_write_new_credentials()
            print("A new token has been generated.")
        
    return credentials


def extract_user_playlists(youtube) -> dict[str, str]:
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
    
    return playlists


def extract_videos(youtube, playlists: dict[str, str]) -> None:
    """
    Populate:
        distinct_videos: dictionary that contains unique videos.
        Key is video id, values are list of title, channel name, description and duration in ms.

        youtube_library: a list that maps playlist id to video id (both are strings).
        `Liked videos` are included as a pseudo playlist with id equal to '0'.
    """
    extract_videos_from_playlists(youtube, playlists)
    extract_liked_videos(youtube)


def extract_videos_from_playlists(youtube, playlists: dict[str, str]) -> None:
    """
    Extract videos from your playlists and store them in distinct_videos and youtube_library.

    Args:
        playlists: iterable object that contains playlist ids
    """
    for playlist_id in playlists:
    
        response = get_playlist_items_page(youtube, playlist_id)
        populate_with_playlist_items_page(response, playlist_id)

        while response.get('nextPageToken', ''):
            response = get_playlist_items_page(youtube, playlist_id, response['nextPageToken'])
            populate_with_playlist_items_page(response, playlist_id)
    
    add_ms_duration(youtube)


def extract_liked_videos(youtube) -> None:
    """
    Extract your liked videos and store them in distinct_videos and youtube_library.
    """
    response = get_liked_videos_page(youtube)
    populate_with_liked_videos_page(response)

    while response.get('nextPageToken', ''):
        response = get_liked_videos_page(youtube, response['nextPageToken'])
        populate_with_liked_videos_page(response)


def get_playlist_items_page(youtube, playlist_id: str, nextPageToken: str = ''):
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


def get_liked_videos_page(youtube, nextPageToken: str = ''):
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


def populate_with_playlist_items_page(response, playlist_id: str) -> None:
    for item in response['items']:
        # Remove deleted and private videos
        if item['snippet']['title'] not in ('Deleted video', 'Private video'):

            distinct_videos[item['contentDetails']['videoId']] = [item['snippet']['title'],
                                                                  item['snippet']['videoOwnerChannelTitle'],
                                                                  item['snippet']['description']]

            youtube_library.append([playlist_id,
                                    item['contentDetails']['videoId']])


def populate_with_liked_videos_page(response):
    for item in response['items']:
        # deleted and private videos actomaticly excluded by YouTube Data API
        iso8601_duration = item['contentDetails']['duration']
        duration_ms = int(aniso8601.parse_duration(iso8601_duration).total_seconds()*1000)

        distinct_videos[item['id']] = [item['snippet']['title'],
                                       item['snippet']['channelTitle'],
                                       item['snippet']['description'],
                                       duration_ms]

        youtube_library.append(['0', # id of Liked videos
                                item['id']])


def split_to_50size_chunks(distinct_videos: dict[str, list[str]]) -> list[list[str]]:
    """
    Split video ids into 50-size chunks.
    """
    chunks: list[list[str]] = []
    chunk: list[str] = []

    for ind, video_id in enumerate(distinct_videos):
        if ind % 50 == 0 and chunk: # not include the first empty chunk
            chunks.append(chunk)
            chunk = []
        chunk.append(video_id)

    chunks.append(chunk) # append the last chunk
    return chunks


def add_ms_duration(youtube) -> None:
    """
    Call videos().list for each chunk in the chunks list.
    Extract video duration, convert it to milliseconds and store it in the distinct_videos.
    """
    chunks = split_to_50size_chunks(distinct_videos)

    str_chunks: list[str] = [','.join(chunk) for chunk in chunks]

    for chunk_ind, str_chunk in enumerate(str_chunks):
        request = youtube.videos().list(part="snippet,contentDetails", id=str_chunk)
        response = request.execute()
        
        for item_ind, item in enumerate(response['items']):
            video_id = chunks[chunk_ind][item_ind] # get the videoId

            iso8601_duration = item['contentDetails']['duration']
            duration_ms = int(aniso8601.parse_duration(iso8601_duration).total_seconds()*1000)
            
            distinct_videos[video_id].append(duration_ms)


def create_df_playlists(playlists: dict[str, str]) -> pd.DataFrame:
    """
    Return a playlists dataframe from a playlist dictionary.
    """
    playlists_series = pd.Series(playlists)
    df_playlists = pd.DataFrame(playlists_series, 
                                columns=['playlist_name']) \
                               .reset_index(names='youtube_playlist_id')
    
    liked = pd.DataFrame({'youtube_playlist_id': '0',
                          'playlist_name': 'Liked videos'}, index = [0])
    
    df_playlists = pd.concat([df_playlists, liked], ignore_index=True)

    return df_playlists


def create_df_videos(distinct_videos: dict[str, list[str]]) -> pd.DataFrame:
    """
    Return a df_videos dataframe from a distinct_videos dictionary.
    """
    df_videos = pd.DataFrame.from_dict(distinct_videos, orient='index',
                                       columns=['youtube_title',
                                                'youtube_channel',
                                                'description',
                                                'duration_ms']) \
                                        .reset_index(names='video_id')
    return df_videos


def create_df_youtube_library(youtube_library: list[list[str]]) -> pd.DataFrame:
    """
    Return a df_youtube_library dataframe from a youtube_library list.
    """
    df_youtube_library = pd.DataFrame(youtube_library,
                                      columns=['youtube_playlist_id',
                                               'video_id']) \
                                        .reset_index(names='id')
    return df_youtube_library


def load_to_bigquery(df: pd.DataFrame, table_name: str, schema: list = None, method: str = 'replace') -> None:
    """
    Upload the dataframe in Google BigQuery.
    Create, replace or append depending on the method passed.
    """
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)
    table_id = f'{project_id}.marts.{table_name}'

    if method == 'replace':
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    elif method == 'append':
        client.load_table_from_dataframe(df, table_id).result()

    else:
        raise Exception('Unexpected method')


if __name__ == '__main__':
    credentials = get_valid_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    playlists = extract_user_playlists(youtube)
    print(f"{len(playlists)} playlists were extracted.")

    df_playlists = create_df_playlists(playlists)
    load_to_bigquery(df_playlists, 'youtube_playlists')
    print(f'youtube_playlists uploaded to BigQuery, {len(df_playlists)} rows.')

    distinct_videos: dict[str, list[str]] = {}
    youtube_library: list[list[str]] = []
    extract_videos(youtube, playlists)

    df_videos = create_df_videos(distinct_videos)
    load_to_bigquery(df_videos, 'youtube_videos')
    print(f'youtube_videos uploaded to BigQuery, {len(df_videos)} rows.')

    df_youtube_library = create_df_youtube_library(youtube_library)
    load_to_bigquery(df_youtube_library, 'youtube_library')
    print(f'youtube_library uploaded to BigQuery, {len(df_youtube_library)} rows.')
