import os
import math
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


def extract_df_videos() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    breakpoint_ms = os.getenv('BREAKPOINT_MS')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        p.playlist_name,
        v.channel_name,
        v.title,
        lower(v.description) description,
        v.duration_ms
    
    FROM `{project_id}.marts.youtube_videos` v

    INNER JOIN `{project_id}.marts.youtube_playlists` p
    ON v.youtube_playlist_id = p.youtube_playlist_id

    WHERE v.duration_ms >= {breakpoint_ms}
    ORDER BY p.playlist_name, v.channel_name, v.title, v.duration_ms
    """

    df_videos = client.query(sql).to_dataframe()
    return df_videos


def get_authorization_code():
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    return sp


def get_current_user_id() -> str:
    user_info = sp.current_user()
    return user_info['id']


def create_playlist_from_df_row(row):
    playlist_info = sp.user_playlist_create(user_id, row['playlist_name'], public=False, collaborative=False)
    return playlist_info['id']


def create_spotify_playlists_from_df(df_playlists: pd.DataFrame) -> pd.DataFrame:
    """
    Create private, non-collaborative Spotify playlists from the dataframe.
    Save created playlist ids as a column in the original dataframe.
    """
    df_playlists['spotify_playlist_id'] = df_playlists.apply(create_playlist_from_df_row, axis = 1)
    return df_playlists


def get_spotify_playlist_id(row) -> str:
    playlist = df_playlists[df_playlists['playlist_name'] == row['playlist_name']]
    if playlist.empty:
        logging.info(f'Spotify id not found for playlist "{row["playlist_name"]}", video "{row["title"]}" skipped.')
        return
    elif len(playlist) > 1:
        logging.info(f'{len(playlist)} spotify ids were found for playlist: "{row["playlist_name"]}", first id was chosen.')
    return playlist.iloc[0, 2]


def get_spotify_track_uri(row) -> str:
    min_diff2 = math.inf
    track_uri, min_diff = search_spotify_track(row['title'], row['duration_ms'])

    if min_diff > 5000: # abs grater than 5 seconds
        print(f'WARNING: Track {row["title"]} not found by title, tring to add channel_name to search')
        track_uri2, min_diff2 = search_spotify_track(f'{row["channel_name"]} {row["title"]}', row['duration_ms'])
    
    return track_uri if min_diff < min_diff2 else track_uri2


def search_spotify_track(q: str, track_duration_ms: int) -> tuple[str, int]:
    results = sp.search(q=q, limit=15, type='track')
    tracks = results['tracks']['items']

    min_diff_track_num = 0
    min_diff = math.inf

    for track_num, track in enumerate(tracks):
        diff = abs(track['duration_ms'] - track_duration_ms)

        if diff <= 5000: # difference in 5 seconds is fine
            print(f'Track found on try: {track_num}')
            return track['uri'], diff
        
        if diff < min_diff:
            min_diff = diff
            min_diff_track_num = track_num
    
    return tracks[min_diff_track_num]['uri'], min_diff


def get_spotify_tracks_uri_from_album_name(row) -> tuple[str, str]:
    
    # First try, just video title
    album_uri, album_tracks_uri = search_spotify_album(row, q=row["title"], limit=2)

    # Second try, album + space + album name in quotes
    if not album_uri:
        q = f'album "{row["title"]}"'
        album_uri, album_tracks_uri = search_spotify_album(row, q=q, limit=2)
    
    return album_uri, album_tracks_uri


def search_spotify_album(row, q: str, limit: int) -> tuple[str, list]:
    albums = sp.search(q=q, limit=limit, type='album')

    for album_num, album in enumerate(albums['albums']['items']):    
        tracks_uri = []
        diff = row['duration_ms']
        tracks_in_desc = 0
        # album_length = 0
        
        tracks = sp.album(album['uri'])
        for track in tracks['tracks']['items']:
            if track['name'].lower() in row['description']: # case-insensitive match
                tracks_in_desc += 1
            
            tracks_uri.append(track['uri'])
            # album_length += track['duration_ms']
            diff -= track['duration_ms']
            if diff < -20000:
                break
        
        percent_in_desc = (tracks_in_desc / len(tracks_uri)) * 100 # in case a albums are same with a diffrence in few tracks
        
        # Difference in 40 seconds is fine or 70%+ tracks in YouTube video description (only if total number of tracks is objective)
        if (abs(diff) < 40000) or (len(tracks_uri) >= 4 and percent_in_desc >= 70):
            print(f'Album "{row["title"]}" found on try {album_num}, ' \
                  f'difference: {round(diff / 1000)} seconds, '
                  f'{tracks_in_desc} of {len(tracks_uri)} track titles ({round(percent_in_desc)}%) in the YouTube video description.')
            
            return album['uri'], tracks_uri
    
    return None, None


def add_videos_to_playlists(row) -> None:
    spotify_playlist_id = get_spotify_playlist_id(row)
    breakpoint_ms = int(os.getenv('BREAKPOINT_MS'))

    # ALBUM
    if row['duration_ms'] >= breakpoint_ms: # a YouTube video is probably a album
        album_uri, tracks_uri = get_spotify_tracks_uri_from_album_name(row)
        if album_uri:
            sp.playlist_add_items(spotify_playlist_id, tracks_uri) # add all album tracks to playlist
            sp.current_user_saved_albums_add([album_uri]) # save the album to current user library
        else:
            print(f'Album "{row["title"]}" not found on Spotify')
    
    # TRACK
    else: # a YouTube video is probably a track
        track_uri = get_spotify_track_uri(row)
        # print(spotify_playlist_id, track_uri)
        sp.playlist_add_items(spotify_playlist_id, [track_uri])


# def add_to_saved_tracks(track_id: str) -> None:
#     sp.current_user_saved_tracks_add(tracks=[track_id])


# def load_to_spotify(row) -> None:
#     item_id = find_spotify_id(row)
#     add_to_saved_tracks(item_id)


if __name__ == '__main__':
    # extract dataframes from BigQuery
    df_playlists = extract_df_playlists()
    df_videos = extract_df_videos()
    print(f'Datasets were extracted from BigQuery.')
    
    # authorisation
    sp = get_authorization_code()
    user_id = get_current_user_id()

    # create and store playlists info
    df_playlists = create_spotify_playlists_from_df(df_playlists)
    print(f'{len(df_playlists)} playlists were added.')

    load_to_bigquery(df_playlists[['spotify_playlist_id', 'playlist_name']], 'spotify_playlists')
    load_to_bigquery(df_playlists[['youtube_playlist_id', 'spotify_playlist_id']], 'playlists_ids')
    print(f'Datasets were loaded to BigQuery.')

    # df_test = df_videos[df_videos['playlist_name'] == os.getenv('PLAYLIST_NAME')]
    df_videos.apply(add_videos_to_playlists, axis = 1)

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
