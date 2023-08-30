import os
import re
import pandas as pd
from dotenv import load_dotenv
import logging
from youtube_etl import load_to_bigquery
from spotify_unlike_tracks import populate_tracks_uri
from spotify_unlike_albums import populate_albums_uri

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


def extract_playlists() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        youtube_playlist_id,
        playlist_name
    
    FROM `{project_id}.marts.youtube_playlists`
    WHERE youtube_playlist_id != '0' -- remove Liked videos as a playlist
    ORDER BY playlist_name
    """

    df_playlists = client.query(sql).to_dataframe()
    return df_playlists


def extract_videos() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    threshold_ms = os.getenv('THRESHOLD_MS')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        v.video_id,
        p.playlist_name,
        v.youtube_channel,
        v.youtube_title,
        lower(v.description) description,
        v.duration_ms
    
    FROM `{project_id}.marts.youtube_videos` v

    LEFT JOIN `{project_id}.marts.youtube_playlists` p
    ON v.youtube_playlist_id = p.youtube_playlist_id

    ORDER BY v.order_num
    """ # WHERE v.duration_ms < {threshold_ms}

    df_videos = client.query(sql).to_dataframe()
    return df_videos


def get_authorization_code():
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    return sp


def get_user_id() -> str:
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
        logging.info(f'Spotify id not found for playlist "{row["playlist_name"]}", video "{row["youtube_title"]}" skipped.')
        return
    
    elif len(playlist) > 1:
        logging.info(f'{len(playlist)} spotify ids were found for playlist: "{row["playlist_name"]}", first id was chosen.')
    
    return playlist.iloc[0, 2]


def find_track(row) -> dict:
    # youtube_title = re.sub('&', 'and', row['youtube_title'])

    # First try, depends on whether it is a Topic Channel
    if ' - Topic' in row['youtube_channel']:
        artist = re.sub(' - Topic', '', row['youtube_channel'])
        artist = re.sub('\'', ' ', artist)

        q = f'track:{row["youtube_title"]} artist:{artist}'
        track_info = qsearch_track(row, q=q, search_type_id='0', limit=2)
    
    else:
        track_info = qsearch_track(row, q=row['youtube_title'], search_type_id='1', limit=2)

    # Second try, track + space + track name in quotes
    if not track_info:
        q = f'track "{row["youtube_title"]}"'
        track_info = qsearch_track(row, q=q, search_type_id='2', limit=2)

    # Third try, channel name + space + track title
    if not track_info:
        artist = re.sub(' - Topic', '', row['youtube_channel'])
        q = f'{artist} {row["youtube_title"]}'
        track_info = qsearch_track(row, q=q, search_type_id='3', limit=2)

    if not track_info:
        print(f'Track "{row["youtube_title"]}" not found on Spotify')
    return track_info


def qsearch_track(row, q: str, search_type_id: str, limit: int) -> dict:
    tracks = sp.search(q=q, limit=limit, type='track')

    for track_num, track in enumerate(tracks['tracks']['items']):
        artists, artists_in_title, track_in_title = [], 0, 0
        diff = abs(track['duration_ms'] - row['duration_ms'])

        for artist in track['artists']:
            artists.append(artist['name'])
            if artist['name'].lower() in row["youtube_title"].lower(): # case-insensitive match
                artists_in_title += 1
        
        if track['name'].lower() in row["youtube_title"].lower():
            track_in_title = 1

        # Difference in 5 seconds or both track name and at least one artist presented in video title
        if diff <= 5000 or (track_in_title and artists_in_title):
            print(f'Track "{row["youtube_title"]}" found on try: {track_num}, ' \
                  f'difference: {round(diff / 1000)} seconds. ')
            
            return {
                'track_uri': track['uri'],
                'album_uri': track['album']['uri'],
                'track_title': track['name'],
                'track_artists': '; '.join(artist for artist in artists),
                'duration_ms': track['duration_ms'],
                'found_on_try': track_num,
                'difference_ms': abs(diff),
                'tracks_in_desc': 1,
                'q': q,
                'search_type_id': search_type_id
            }


def save_track(track_info: dict, spotify_playlist_id: str, video_title: str) -> str:
    if (track_info['track_uri'], spotify_playlist_id) in ((uri, playlist_id) for uri, playlist_id, *_ in spotify_log): # search with primary key
        status = 'skipped (saved during the run)'
        print(f'WARNING: Track "{video_title}" skipped (saved during the run)')

    elif track_info['track_uri'] in liked_tracks_uri and not spotify_playlist_id:
        status = 'skipped (saved before the run)'
        print(f'WARNING: Track "{video_title}" skipped (saved before the run)')
    
    else:
        status = 'saved'
        if spotify_playlist_id:
            # Add the track to the playlist
            sp.playlist_add_items(spotify_playlist_id, [track_info['track_uri']])
        
        else:
            # Like the track
            sp.current_user_saved_tracks_add([track_info['track_uri']])
    
    return status


def log_track(track_info: dict, spotify_playlist_id: str, video_id: str, status: str) -> None:
    # if track['uri'] not in spotify_tracks: # condition doesn't matter
    spotify_tracks[track_info['track_uri']] = (track_info['album_uri'],
                                               track_info['track_title'],
                                               track_info['track_artists'],
                                               track_info['duration_ms'])
    
    spotify_log.append((track_info['track_uri'],
                        spotify_playlist_id,
                        video_id,
                        # 1, # uri_type
                        track_info['found_on_try'],
                        track_info['difference_ms'],
                        track_info['tracks_in_desc'],
                        track_info['q'],
                        track_info['search_type_id'],
                        status))


def find_album(row) -> dict:
    # First try, just video title
    album_info = qsearch_album(row, q=row["youtube_title"], search_type_id='1', limit=2)

    # Second try, album + space + album name in quotes
    if not album_info:
        q = f'album "{row["youtube_title"]}"'
        album_info = qsearch_album(row, q=q, search_type_id='2', limit=2)

    # if not album_info:
    #     print(f'Album "{row["youtube_title"]}" not found on Spotify')
    return album_info


def qsearch_album(row, q: str, search_type_id: str, limit: int) -> dict:
    albums = sp.search(q=q, limit=limit, type='album')

    for album_num, album in enumerate(albums['albums']['items']):
        tracks_uri: list[str] = [] # track uri
        tracks_info: list[tuple[str, str, int]] = [] # (track uri, track title, track duration ms)
        diff = row['duration_ms']
        tracks_in_desc = 0
        album_length = 0
        
        tracks = sp.album(album['uri'])
        for track in tracks['tracks']['items']:
            if track['name'].lower() in row['description']: # case-insensitive match
                tracks_in_desc += 1
            
            tracks_uri.append(track['uri'])
            tracks_info.append((track['uri'], track['name'], track['duration_ms']))
            
            album_length += track['duration_ms']
            diff -= track['duration_ms']
            if diff < -20000:
                break
        
        percent_in_desc = (tracks_in_desc / len(tracks_uri)) * 100 # in case a albums are same with a diffrence in few tracks
        
        # Difference in 40 seconds or 70%+ tracks found in the YouTube video description (only if the total number of tracks is objective)
        if (abs(diff) < 40000) or (len(tracks_uri) >= 4 and percent_in_desc >= 70):
            print(f'Album "{row["youtube_title"]}" found on try {album_num}, '
                  f'difference: {round(diff / 1000)} seconds, '
                  f'{tracks_in_desc} of {len(tracks_uri)} track titles '
                  f'({round(percent_in_desc)}%) found in the YouTube video description.')
            
            return {
                'album_uri': album['uri'],
                'tracks_uri': tracks_uri,
                'tracks_info': tracks_info,
                'album_title': album['name'],
                'album_artists': '; '.join(artist['name'] for artist in album['artists']),
                'duration_ms': album_length,
                'total_tracks': len(tracks_uri),
                'found_on_try': album_num,
                'difference_ms': abs(diff),
                'tracks_in_desc': tracks_in_desc,
                'q': q,
                'search_type_id': search_type_id
            }


def save_album(album_info: dict, spotify_playlist_id: str, video_title: str) -> str:
    if (album_info['album_uri'], spotify_playlist_id) in ((uri, playlist_id) for uri, playlist_id, *_ in spotify_log): # search with primary key
        status = 'skipped (saved during the run)'
        print(f'WARNING: Album "{video_title}" skipped (saved during the run)')
    
    elif album_info['album_uri'] in liked_albums_uri and not spotify_playlist_id:
        status = 'skipped (saved before the run)'
        print(f'WARNING: Album "{video_title}" skipped (saved before the run)')

    else:
        status = 'saved'
        if spotify_playlist_id:
            # Add album tracks not present in the playlist to the playlist
            sp.playlist_add_items(spotify_playlist_id, album_info['tracks_uri'])

            # Save the album to current user library
            # sp.current_user_saved_albums_add([album_info['album_uri']])
        
        else:
            # Like all tracks in the album
            # sp.current_user_saved_tracks_add(album_info['tracks_uri'])

            # Save the album to current user library
            sp.current_user_saved_albums_add([album_info['album_uri']])

    return status


def log_album(album_info: dict, spotify_playlist_id: str, video_id: str, status: str) -> None:
    # if album_info['album_uri'] not in spotify_albums:  # condition doesn't matter
    spotify_albums[album_info['album_uri']] = (album_info['album_title'],
                                               album_info['album_artists'],
                                               album_info['duration_ms'],
                                               album_info['total_tracks'])
    
    for track_uri, title, duration_ms in album_info['tracks_info']:
        # if track_uri not in spotify_tracks: # condition doesn't matter
        spotify_tracks[track_uri] = (album_info['album_uri'],
                                     title,
                                     # Same as album artists, not always correct,
                                     # but we don't iterate for every artist on every track.
                                     album_info['album_artists'], 
                                     duration_ms)
    
    spotify_log.append((album_info['album_uri'],
                        spotify_playlist_id,
                        video_id,
                        # 0, # uri_type
                        album_info['found_on_try'],
                        album_info['difference_ms'],
                        album_info['tracks_in_desc'],
                        album_info['q'],
                        album_info['search_type_id'],
                        status))


def find_other_playlist(row) -> dict:
    # First try, just video title
    playlist_info = qsearch_playlist(row, q=row["youtube_title"], search_type_id='1', limit=2)

    # Second try, playlist + space + playlist name in quotes
    if not playlist_info:
        q = f'playlist "{row["youtube_title"]}"'
        playlist_info = qsearch_playlist(row, q=q, search_type_id='2', limit=2)
    
    # Third try, channel name + space + playlist title
    if not playlist_info:
        q = f'{row["youtube_channel"]} {row["youtube_title"]}'
        playlist_info = qsearch_playlist(row, q=q, search_type_id='3', limit=2)

    if not playlist_info:
        print(f'Album/Playlist "{row["youtube_title"]}" not found on Spotify')
    return playlist_info


def qsearch_playlist(row, q: str, search_type_id: str, limit: int) -> dict:
    playlists = sp.search(q=q, limit=limit, type='playlist')

    for playlist_num, playlist in enumerate(playlists['playlists']['items']):
        tracks_uri: list[str] = [] # track uri
        tracks_info: list[tuple[str, str, int]] = [] # (track uri, track title, track duration ms)
        diff = row['duration_ms']
        tracks_in_desc = 0
        playlist_length = 0
        
        tracks = sp.playlist(playlist['uri'])
        for track in tracks['tracks']['items']:
            if track['track']['name'].lower() in row['description']: # case-insensitive match
                tracks_in_desc += 1
            
            tracks_uri.append(track['track']['uri'])
            tracks_info.append((track['track']['uri'], track['track']['name'], track['track']['duration_ms']))
            
            playlist_length += track['track']['duration_ms']
            diff -= track['track']['duration_ms']
            if diff < -20000:
                break
        
        percent_in_desc = (tracks_in_desc / len(tracks_uri)) * 100 # in case a playlist are same with a diffrence in few tracks
        
        # Difference in 40 seconds or 70%+ tracks found in the YouTube video description (only if the total number of tracks is objective)
        if (abs(diff) < 40000) or (len(tracks_uri) >= 4 and percent_in_desc >= 70):
            print(f'Playlist "{row["youtube_title"]}" found on try {playlist_num}, '
                  f'difference: {round(diff / 1000)} seconds, '
                  f'{tracks_in_desc} of {len(tracks_uri)} track titles '
                  f'({round(percent_in_desc)}%) found in the YouTube video description.')
            
            return {
                'playlist_uri': playlist['uri'],
                'playlist_id': playlist['id'],
                'tracks_uri': tracks_uri,
                'tracks_info': tracks_info,
                'playlist_title': playlist['name'],
                'playlist_owner': playlist['owner']['display_name'],
                'duration_ms': playlist_length,
                'total_tracks': len(tracks_uri),
                'found_on_try': playlist_num,
                'difference_ms': abs(diff),
                'tracks_in_desc': tracks_in_desc,
                'q': q,
                'search_type_id': search_type_id
            }


def save_other_playlist(playlist_info: dict, spotify_playlist_id: str, video_title: str) -> str:
    if (playlist_info['playlist_uri'], spotify_playlist_id) not in ((uri, playlist_id) for uri, playlist_id, *_ in spotify_log): # search with primary key
        status = 'saved'
        if spotify_playlist_id:
            # Add playlist tracks not present in the current user playlist to the current user playlist
            sp.playlist_add_items(spotify_playlist_id, playlist_info['tracks_uri'])

            # Save the playlist to current user library
            # sp.current_user_follow_playlist(playlist_info['playlist_id'])
        
        else:
            # Like all tracks in the playlist
            # sp.current_user_saved_tracks_add(playlist_info['tracks_uri'])

            # Save the playlist to current user library
            sp.current_user_follow_playlist(playlist_info['playlist_id'])

    else:
        status = 'skipped (saved during the run)'
        print(f'WARNING: Playlist "{video_title}" skipped (saved during the run)')

    return status


def log_other_playlist(playlist_info: dict, spotify_playlist_id: str, video_id: str, status: str) -> None:
    spotify_playlists_others[playlist_info['playlist_uri']] = (playlist_info['playlist_title'],
                                                               playlist_info['playlist_owner'],
                                                               playlist_info['duration_ms'],
                                                               playlist_info['total_tracks'])
    
    for track_uri, title, duration_ms in playlist_info['tracks_info']:
        # if track_uri not in spotify_tracks: # condition doesn't matter
        spotify_tracks[track_uri] = (playlist_info['playlist_uri'],
                                     title,
                                     # Same as album artists, not always correct,
                                     # but we don't iterate for every artist on every track.
                                     playlist_info['playlist_owner'], # TODO: fix
                                     duration_ms)
    
    spotify_log.append((playlist_info['playlist_uri'],
                        spotify_playlist_id,
                        video_id,
                        # 2, # uri_type
                        playlist_info['found_on_try'],
                        playlist_info['difference_ms'],
                        playlist_info['tracks_in_desc'],
                        playlist_info['q'],
                        playlist_info['search_type_id'],
                        status))


def populate_spotify(row) -> None:
    """
    Find albums and tracks on Spotify, like or add them to the created playlists.

    Return:
        dict: a skeleton for df_spotify_catalog dataframe.
        If the first video is not found and populate_playlists returns None,
        df_spotify_catalog will be a Series.
    """
    spotify_playlist_id = None
    if row['playlist_name']:
        spotify_playlist_id = get_spotify_playlist_id(row)

    # ALBUM OR PLAYLIST
    # THRESHOLD_MS is specified and the duration of the video is greater than or equal to it
    if os.getenv('THRESHOLD_MS') and row['duration_ms'] >= int(os.getenv('THRESHOLD_MS')):
        album_info = find_album(row)
        if album_info:
            status = save_album(album_info, spotify_playlist_id, row['youtube_title'])
            log_album(album_info, spotify_playlist_id, row['video_id'], status)
        else:
            playlist_info = find_other_playlist(row)
            if playlist_info:
                status = save_other_playlist(playlist_info, spotify_playlist_id, row['youtube_title'])
                log_other_playlist(playlist_info, spotify_playlist_id, row['video_id'], status)

    # TRACK
    # either THRESHOLD_MS is not specified or the duration of the video is less than it
    else:
        track_info = find_track(row)
        if track_info:
            status = save_track(track_info, spotify_playlist_id, row['youtube_title'])
            log_track(track_info, spotify_playlist_id, row['video_id'], status)


def spotify_albums_to_df(spotify_albums: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify album dataframe from a album dictionary.
    """
    df_spotify_albums = pd.DataFrame.from_dict(spotify_albums, orient='index',
                                               columns=['album_title',
                                                        'album_artists',
                                                        'duration_ms',
                                                        'total_tracks']) \
                                                .reset_index(names='album_uri')
    return df_spotify_albums


def spotify_playlists_others_to_df(spotify_playlists_others: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify album dataframe from a album dictionary.
    """
    df_spotify_playlists_others = pd.DataFrame.from_dict(spotify_playlists_others, orient='index',
                                                         columns=['playlist_title',
                                                                  'playlist_owner',
                                                                  'duration_ms',
                                                                  'total_tracks']) \
                                                        .reset_index(names='playlist_uri')
    return df_spotify_playlists_others


def spotify_tracks_to_df(spotify_tracks: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify track dataframe from a track dictionary.
    """
    df_spotify_tracks = pd.DataFrame.from_dict(spotify_tracks, orient='index',
                                               columns=['album_uri',
                                                        'track_title',
                                                        'track_artists',
                                                        'duration_ms']) \
                                                .reset_index(names='track_uri')
    return df_spotify_tracks


def spotify_log_to_df(spotify_log: list[tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify log dataframe from a log list.
    """
    df_spotify_log = pd.DataFrame(spotify_log, columns=['spotify_uri',
                                                        'spotify_playlist_id',
                                                        'youtube_video_id',
                                                        # 'uri_type',
                                                        'found_on_try',
                                                        'difference_ms',
                                                        'tracks_in_desc',
                                                        'q',
                                                        'search_type_id',
                                                        'status'])

    return df_spotify_log


def create_df_search_types() -> pd.DataFrame:
    search_types = {'0': 'colons',
                    '1': 'title only',
                    '2': 'keyword and quotes',
                    '3': 'channel name and title'}
    
    df_search_types = pd.DataFrame.from_dict(search_types, orient='index',
                                            columns=['search_type_name']) \
                                            .reset_index(names='search_type_id')
    
    return df_search_types


def create_df_spotify_playlists(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_spotify_playlists = df_playlists[['spotify_playlist_id', 'playlist_name']]
    liked = pd.DataFrame({'spotify_playlist_id': '0',
                          'playlist_name': 'Liked'}, index = [0])
    
    df_spotify_playlists = pd.concat([df_spotify_playlists, liked], ignore_index=True)

    return df_spotify_playlists


def create_df_playlist_ids(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_playlist_ids = df_playlists[['youtube_playlist_id', 'spotify_playlist_id']]
    liked_videos = pd.DataFrame({'youtube_playlist_id': '0',
                                 'spotify_playlist_id': '0'}, index = [0])

    df_playlist_ids = pd.concat([df_playlist_ids, liked_videos], ignore_index=True)
    df_playlist_ids = df_playlist_ids.reset_index(names='id')

    return df_playlist_ids


if __name__ == '__main__':
    # Extract dataframes from BigQuery.
    df_playlists = extract_playlists()
    df_videos = extract_videos()
    print(f'Datasets were extracted from BigQuery.')

    # Authorisation.
    sp = get_authorization_code()
    user_id = get_user_id()

    '''
    Extract liked tracks and albums to prevent "overlike".
    Without it, you will lose overliked tracks and albums by using spotify_unlike scripts
    (the added_at field will be overwritten)
    '''
    liked_tracks_uri = populate_tracks_uri(sp)
    liked_albums_uri = populate_albums_uri(sp)
    print('Tracks and albums URI were collected.')

    # Create Spotify playlists.
    df_playlists['spotify_playlist_id'] = df_playlists.apply(create_spotify_playlists_from_df, axis = 1)
    print(f'{len(df_playlists)} playlists were added.')

    df_spotify_playlists = create_df_spotify_playlists(df_playlists)
    load_to_bigquery(df_spotify_playlists, 'spotify_playlists', 'replace')
    print(f'spotify_playlists uploaded to BigQuery.')

    df_playlist_ids = create_df_playlist_ids(df_playlists)
    load_to_bigquery(df_playlist_ids, 'playlist_ids', 'replace')
    print(f'playlist_ids uploaded to BigQuery.')

    # Populate Spotify.
    spotify_albums: dict[str, tuple[str]] = {}
    spotify_playlists_others: dict[str, tuple[str]] = {}
    spotify_tracks: dict[str, tuple[str]] = {}
    spotify_log: list[tuple[str]] = []

    df_videos.apply(populate_spotify, axis = 1)

    df_spotify_albums = spotify_albums_to_df(spotify_albums)
    load_to_bigquery(df_spotify_albums, 'spotify_albums', 'replace')
    print(f'spotify_albums uploaded to BigQuery, {len(df_spotify_albums)} rows.')

    df_spotify_playlists_others = spotify_playlists_others_to_df(spotify_playlists_others)
    load_to_bigquery(df_spotify_playlists_others, 'spotify_playlists_others', 'replace')
    print(f'spotify_playlists_others uploaded to BigQuery, {len(df_spotify_playlists_others)} rows.')

    df_spotify_tracks = spotify_tracks_to_df(spotify_tracks)
    load_to_bigquery(df_spotify_tracks, 'spotify_tracks', 'replace')
    print(f'spotify_tracks uploaded to BigQuery, {len(df_spotify_tracks)} rows.')

    df_spotify_log = spotify_log_to_df(spotify_log)
    load_to_bigquery(df_spotify_log, 'spotify_log', 'replace')
    print(f'spotify_log uploaded to BigQuery, {len(df_spotify_log)} rows.')

    # Create search types.
    df_search_types = create_df_search_types()
    load_to_bigquery(df_search_types, 'search_types', 'replace')
    print(f'search_types uploaded to BigQuery.')


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
