import os
import re
import pandas as pd
from dotenv import load_dotenv
import logging
from youtube_etl import load_to_bigquery

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


def extract_df_playlists() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        youtube_playlist_id,
        playlist_name
    
    FROM `{project_id}.marts.youtube_playlists`
    ORDER BY playlist_name
    """

    df_playlists = client.query(sql).to_dataframe()
    return df_playlists


def extract_df_youtube_videos() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    # playlist_name = os.getenv('PLAYLIST_NAME')
    breakpoint_ms = os.getenv('BREAKPOINT_MS')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        v.video_id,
        p.playlist_name,
        v.channel_name,
        v.title,
        lower(v.description) description,
        v.duration_ms
    
    FROM `{project_id}.marts.youtube_videos` v

    INNER JOIN `{project_id}.marts.youtube_playlists` p
    ON v.youtube_playlist_id = p.youtube_playlist_id

    WHERE v.duration_ms < {breakpoint_ms}
    ORDER BY p.playlist_name, v.channel_name, v.title, v.duration_ms
    """

    df_youtube_videos = client.query(sql).to_dataframe()
    return df_youtube_videos


def get_authorization_code():
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    return sp


def get_current_user_id() -> str:
    user_info = sp.current_user()
    return user_info['id']


def create_spotify_playlists_from_df(row) -> str:
    """
    Create private, non-collaborative Spotify playlists from the dataframe.
    Save created playlist ids as a column in the original dataframe.
    """
    playlist = sp.user_playlist_create(user_id, row['playlist_name'], public=False, collaborative=False)
    return playlist['id']


def get_spotify_playlist_id(row) -> str:
    playlist = df_playlists[df_playlists['playlist_name'] == row['playlist_name']]
    if playlist.empty:
        logging.info(f'Spotify id not found for playlist "{row["playlist_name"]}", video "{row["title"]}" skipped.')
        return
    elif len(playlist) > 1:
        logging.info(f'{len(playlist)} spotify ids were found for playlist: "{row["playlist_name"]}", first id was chosen.')
    return playlist.iloc[0, 2]


def get_spotify_track_uri(row) -> tuple[str, dict]:
    # title = re.sub('&', 'and', row['title'])

    # First try, depends on whether it is a Topic Channel
    if ' - Topic' in row['channel_name']:
        artist = re.sub(' - Topic', '', row['channel_name'])
        artist = re.sub('\'', ' ', artist)

        q = f'track:{row["title"]} artist:{artist}'
        track_uri, track_info = search_spotify_track(row, q=q, search_type_id='0', limit=2)
    
    else:
        track_uri, track_info = search_spotify_track(row, q=row['title'], search_type_id='1', limit=2)

    # Second try, track + space + track name in quotes
    if not track_uri:
        q = f'track "{row["title"]}"'
        track_uri, track_info = search_spotify_track(row, q=q, search_type_id='2', limit=2)

    # Third try, channel name + space + track title
    if not track_uri:
        artist = re.sub(' - Topic', '', row['channel_name'])
        q = f'{artist} {row["title"]}'
        track_uri, track_info = search_spotify_track(row, q=q, search_type_id='3', limit=2)
    
    return track_uri, track_info


def search_spotify_track(row, q: str, search_type_id: str, limit: int) -> tuple[str, dict]:
    tracks = sp.search(q=q, limit=limit, type='track')

    for track_num, track in enumerate(tracks['tracks']['items']):
        diff = abs(track['duration_ms'] - row['duration_ms'])

        if diff <= 5000: # difference in 5 seconds is fine
            print(f'Track "{row["title"]}" found on try: {track_num}, ' \
                  f'difference: {round(diff / 1000)} seconds. ')
            
            return track['uri'], dict({'title': track['name'],
                                       'artists': '; '.join(artist['name'] for artist in track['artists']),
                                       'duration_ms': str(track['duration_ms']),
                                       'found_on_try': str(track_num),
                                       'difference_ms': str(abs(diff)),
                                       'q': q,
                                       'search_type_id': search_type_id})
    
    return None, None


def get_spotify_tracks_uri_from_album_name(row) -> tuple[str, list, dict]:
    
    # First try, just video title
    album_uri, album_tracks_uri, album_info = search_spotify_album(row, q=row["title"], search_type_id='1', limit=2)

    # Second try, album + space + album name in quotes
    if not album_uri:
        q = f'album "{row["title"]}"'
        album_uri, album_tracks_uri, album_info = search_spotify_album(row, q=q, search_type_id='2', limit=2)
    
    return album_uri, album_tracks_uri, album_info


def search_spotify_album(row, q: str, search_type_id: str, limit: int) -> tuple[str, list, dict]:
    albums = sp.search(q=q, limit=limit, type='album')

    for album_num, album in enumerate(albums['albums']['items']):    
        tracks_uri = []
        diff = row['duration_ms']
        tracks_in_desc = 0
        album_length = 0
        
        tracks = sp.album(album['uri'])
        for track in tracks['tracks']['items']:
            if track['name'].lower() in row['description']: # case-insensitive match
                tracks_in_desc += 1
            
            tracks_uri.append(track['uri'])
            album_length += track['duration_ms']
            diff -= track['duration_ms']
            if diff < -20000:
                break
        
        percent_in_desc = (tracks_in_desc / len(tracks_uri)) * 100 # in case a albums are same with a diffrence in few tracks
        
        # Difference in 40 seconds or 70%+ tracks in the YouTube video description (only if the total number of tracks is objective) are fine
        if (abs(diff) < 40000) or (len(tracks_uri) >= 4 and percent_in_desc >= 70):
            print(f'Album "{row["title"]}" found on try {album_num}, ' \
                  f'difference: {round(diff / 1000)} seconds, '
                  f'{tracks_in_desc} of {len(tracks_uri)} track titles ({round(percent_in_desc)}%) in the YouTube video description.')

            return album['uri'], tracks_uri, dict({'title': album['name'],
                                                   'artists': '; '.join(artist['name'] for artist in album['artists']),
                                                   'duration_ms': str(album_length),
                                                   'found_on_try': str(album_num),
                                                   'difference_ms': str(abs(diff)),
                                                   'tracks_in_desc': str(tracks_in_desc),
                                                   'total_tracks': str(len(tracks_uri)),
                                                   'q': q,
                                                   'search_type_id': search_type_id})
    
    return None, None, None


def add_videos_to_playlists(row) -> dict:
    """
    Find albums and videos on Spotify and add them to created playlists.

    Return:
        dict: a skeleton for df_spotify_catalog dataframe.
        If the first video is not found and add_videos_to_playlists returns None, df_spotify_catalog will be a Series.
    """
    spotify_playlist_id = get_spotify_playlist_id(row)
    breakpoint_ms = int(os.getenv('BREAKPOINT_MS'))

    # ALBUM
    if row['duration_ms'] >= breakpoint_ms: # a YouTube video is probably a album
        album_uri, album_tracks_uri, album_info = get_spotify_tracks_uri_from_album_name(row)
        if album_uri:
            sp.playlist_add_items(spotify_playlist_id, album_tracks_uri) # add all album tracks to playlist
            # sp.current_user_saved_albums_add([album_uri]) # save the album to current user library

            return dict(dict({'spotify_uri': album_uri,
                              'spotify_playlist_id': spotify_playlist_id,
                              'category': '0'}),
                              **album_info)
        else:
            print(f'Album "{row["title"]}" not found on Spotify')
    
    # TRACK
    else: # a YouTube video is probably a track
        track_uri, track_info = get_spotify_track_uri(row)
        if track_uri:
            sp.playlist_add_items(spotify_playlist_id, [track_uri]) # add all tracks to playlist

            return dict(dict({'spotify_uri': track_uri,
                              'spotify_playlist_id': spotify_playlist_id,
                              'category': '1'}),
                              **track_info)
        else:
            print(f'Track "{row["title"]}" not found on Spotify')
    
    return dict()


# def add_to_saved_tracks(track_id: str) -> None:
#     sp.current_user_saved_tracks_add(tracks=[track_id])


if __name__ == '__main__':
    # extract dataframes from BigQuery
    df_playlists = extract_df_playlists()
    df_youtube_videos = extract_df_youtube_videos()
    print(f'Datasets were extracted from BigQuery.')
    
    # authorisation
    sp = get_authorization_code()
    user_id = get_current_user_id()

    # create the dataframe from Spotify playlists and store it in BigQuery
    df_playlists['spotify_playlist_id'] = df_playlists.apply(create_spotify_playlists_from_df, axis = 1)
    print(f'{len(df_playlists)} playlists were added.')

    load_to_bigquery(df_playlists[['spotify_playlist_id', 'playlist_name']], 'spotify_playlists')
    print(f'spotify_playlists uploaded to BigQuery.')
    load_to_bigquery(df_playlists[['youtube_playlist_id', 'spotify_playlist_id']], 'playlists_ids')
    print(f'playlists_ids uploaded to BigQuery.')

    # create the dataframe for Spotify albums and tracks and store it in BigQuery
    df_spotify_catalog = df_youtube_videos.apply(add_videos_to_playlists, axis = 1, result_type='expand')
    df_spotify_catalog.insert(1, 'youtube_video_id', df_youtube_videos['video_id'])
    df_spotify_catalog = df_spotify_catalog.dropna(how = 'all')
    
    load_to_bigquery(df_spotify_catalog, 'spotify_catalog')
    print(f'spotify_catalog uploaded to BigQuery.')

    # create and upload search types
    search_types = {'0': 'colons',
                    '1': 'title only',
                    '2': 'keyword and quotes',
                    '3': 'channel name and title'}
    
    df_search_types = pd.DataFrame.from_dict(search_types, orient='index',
                                             columns=['search_type_name']) \
                                            .reset_index(names='search_type_id')
    load_to_bigquery(df_search_types, 'search_types')
    print(f'search_types uploaded to BigQuery.')

    #df.apply(load_to_spotify, axis = 1)



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
