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


load_dotenv()


def extract_playlists() -> pd.DataFrame:
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


def extract_videos() -> pd.DataFrame:
    project_id = os.getenv('PROJECT_ID')
    threshold_ms = os.getenv('THRESHOLD_MS')
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        yl.id as log_id,
        yp.playlist_name,

        yv.youtube_title,
        yv.youtube_channel,
        lower(yv.description) description,
        yv.duration_ms
    
    FROM `{project_id}.marts.youtube_library` yl

    INNER JOIN `{project_id}.marts.youtube_playlists` yp ON yl.youtube_playlist_id = yp.youtube_playlist_id 
    INNER JOIN `{project_id}.marts.youtube_videos` yv ON yl.video_id = yv.video_id

    ORDER BY yl.id
    """ # WHERE yv.duration_ms < {threshold_ms}

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
    if row['youtube_playlist_id'] != '0':
        playlist = sp.user_playlist_create(user_id, row['playlist_name'], public=False, collaborative=False)
        return playlist['id']
    else:
        return '0'


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
        track_info = qsearch_track(row, q=q, search_type_id=0, limit=2)
    
    else:
        track_info = qsearch_track(row, q=row['youtube_title'], search_type_id=1, limit=2)

    # Second try, track + space + track name in quotes
    if not track_info:
        q = f'track "{row["youtube_title"]}"'
        track_info = qsearch_track(row, q=q, search_type_id=2, limit=2)

    # Third try, channel name + space + track title
    if not track_info:
        artist = re.sub(' - Topic', '', row['youtube_channel'])
        q = f'{artist} {row["youtube_title"]}'
        track_info = qsearch_track(row, q=q, search_type_id=3, limit=2)

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

    elif track_info['track_uri'] in liked_tracks_uri and spotify_playlist_id == '0':
        status = 'skipped (saved before the run)'
        print(f'WARNING: Track "{video_title}" skipped (saved before the run)')
    
    else:
        status = 'saved'
        if spotify_playlist_id != '0':
            # Add the track to the playlist
            sp.playlist_add_items(spotify_playlist_id, [track_info['track_uri']])
        
        else:
            # Like the track
            sp.current_user_saved_tracks_add([track_info['track_uri']])
    
    return status


def log_track(track_info: dict, spotify_playlist_id: str, log_id: str, status: str) -> None:
    spotify_tracks[track_info['track_uri']] = (track_info['album_uri'],
                                               None,
                                               track_info['track_title'],
                                               track_info['track_artists'],
                                               track_info['duration_ms'])
    
    spotify_log.append((log_id,
                        track_info['track_uri'],
                        spotify_playlist_id,
                        # 1, # uri_type
                        track_info['found_on_try'],
                        track_info['difference_ms'],
                        track_info['tracks_in_desc'],
                        track_info['q'],
                        track_info['search_type_id'],
                        status))


def find_album(row) -> dict:
    # First try, just video title
    album_info = qsearch_album(row, q=row["youtube_title"], search_type_id=1, limit=2)

    # Second try, album + space + album name in quotes
    if not album_info:
        q = f'album "{row["youtube_title"]}"'
        album_info = qsearch_album(row, q=q, search_type_id=2, limit=2)

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
    
    elif album_info['album_uri'] in liked_albums_uri and spotify_playlist_id == '0':
        status = 'skipped (saved before the run)'
        print(f'WARNING: Album "{video_title}" skipped (saved before the run)')

    else:
        status = 'saved'
        if spotify_playlist_id != '0':
            # Add album tracks not present in the playlist to the playlist
            sp.playlist_add_items(spotify_playlist_id, album_info['tracks_uri'])

            # Save the album to current user library
            # sp.current_user_saved_albums_add([album_info['album_uri']])
        
        else:
            # Like all tracks in the album (may cause overlike)
            # sp.current_user_saved_tracks_add(album_info['tracks_uri'])

            # Save the album to current user library
            sp.current_user_saved_albums_add([album_info['album_uri']])

    return status


def log_album(album_info: dict, spotify_playlist_id: str, log_id: str, status: str) -> None:
    spotify_albums[album_info['album_uri']] = (album_info['album_title'],
                                               album_info['album_artists'],
                                               album_info['duration_ms'],
                                               album_info['total_tracks'])
    
    for track_uri, title, duration_ms in album_info['tracks_info']:
        spotify_tracks[track_uri] = (album_info['album_uri'],
                                     None,
                                     title,
                                     # Same as album artists, not always correct,
                                     # but we don't iterate for every artist on every track.
                                     album_info['album_artists'], 
                                     duration_ms)
    
    spotify_log.append((log_id,
                        album_info['album_uri'],
                        spotify_playlist_id,
                        # 0, # uri_type
                        album_info['found_on_try'],
                        album_info['difference_ms'],
                        album_info['tracks_in_desc'],
                        album_info['q'],
                        album_info['search_type_id'],
                        status))


def find_other_playlist(row) -> dict:
    # First try, just video title
    playlist_info = qsearch_playlist(row, q=row["youtube_title"], search_type_id=1, limit=2)

    # Second try, playlist + space + playlist name in quotes
    if not playlist_info:
        q = f'playlist "{row["youtube_title"]}"'
        playlist_info = qsearch_playlist(row, q=q, search_type_id=2, limit=2)
    
    # Third try, channel name + space + playlist title
    if not playlist_info:
        q = f'{row["youtube_channel"]} {row["youtube_title"]}'
        playlist_info = qsearch_playlist(row, q=q, search_type_id=3, limit=2)

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
            if track.get('track', ''):
                if track['track']['name'].lower() in row['description']: # case-insensitive match
                    tracks_in_desc += 1
                
                tracks_uri.append(track['track']['uri'])
                tracks_info.append((track['track']['uri'],
                                    track['track']['name'],
                                    track['track']['duration_ms'],
                                    track['track']['album']['uri']))
                
                playlist_length += track['track']['duration_ms']
                diff -= track['track']['duration_ms']
                if diff < -20000:
                    break

        total_tracks = len(tracks_uri)
        percent_in_desc = (tracks_in_desc / total_tracks) * 100 # in case a playlist are same with a diffrence in few tracks
        
        # Difference in 40 seconds or 70%+ tracks found in the YouTube video description (only if the total number of tracks is objective)
        if (abs(diff) < 40000) or (total_tracks >= 4 and percent_in_desc >= 70):
            print(f'Playlist "{row["youtube_title"]}" found on try {playlist_num}, '
                  f'difference: {round(diff / 1000)} seconds, '
                  f'{tracks_in_desc} of {total_tracks} track titles '
                  f'({round(percent_in_desc)}%) found in the YouTube video description.')
            
            return {
                'playlist_uri': playlist['uri'],
                'playlist_id': playlist['id'],
                'tracks_uri': tracks_uri,
                'tracks_info': tracks_info,
                'playlist_title': playlist['name'],
                'playlist_owner': playlist['owner']['display_name'],
                'duration_ms': playlist_length,
                'total_tracks': total_tracks,
                'found_on_try': playlist_num,
                'difference_ms': abs(diff),
                'tracks_in_desc': tracks_in_desc,
                'q': q,
                'search_type_id': search_type_id
            }


def save_other_playlist(playlist_info: dict, spotify_playlist_id: str, video_title: str) -> str:
    if (playlist_info['playlist_uri'], spotify_playlist_id) not in ((uri, playlist_id) for uri, playlist_id, *_ in spotify_log): # search with primary key
        status = 'saved'
        if spotify_playlist_id != '0':
            # Add playlist tracks not present in the current user playlist to the current user playlist
            sp.playlist_add_items(spotify_playlist_id, playlist_info['tracks_uri'])

            # Save the playlist to current user library
            # sp.current_user_follow_playlist(playlist_info['playlist_id'])
        
        else:
            # Like all tracks in the playlist (may cause overlike)
            # sp.current_user_saved_tracks_add(playlist_info['tracks_uri'])

            # Save the playlist to current user library
            sp.current_user_follow_playlist(playlist_info['playlist_id'])

    else:
        status = 'skipped (saved during the run)'
        print(f'WARNING: Playlist "{video_title}" skipped (saved during the run)')

    return status


def log_other_playlist(playlist_info: dict, spotify_playlist_id: str, log_id: str, status: str) -> None:
    spotify_playlists_others[playlist_info['playlist_uri']] = (playlist_info['playlist_title'],
                                                               playlist_info['playlist_owner'],
                                                               playlist_info['duration_ms'],
                                                               playlist_info['total_tracks'])
    
    for track_uri, title, duration_ms, album_uri in playlist_info['tracks_info']:
        spotify_tracks[track_uri] = (album_uri,
                                     playlist_info['playlist_uri'],
                                     title,
                                     # Same as album artists, not always correct,
                                     # but we don't iterate for every artist on every track.
                                     playlist_info['playlist_owner'], # TODO: fix
                                     duration_ms)
    
    spotify_log.append((log_id,
                        playlist_info['playlist_uri'],
                        spotify_playlist_id,
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
    spotify_playlist_id = get_spotify_playlist_id(row)

    # ALBUM OR PLAYLIST
    # THRESHOLD_MS is specified and the duration of the video is greater than or equal to it
    if os.getenv('THRESHOLD_MS') and row['duration_ms'] >= int(os.getenv('THRESHOLD_MS')):
        album_info = find_album(row)
        if album_info:
            status = save_album(album_info, spotify_playlist_id, row['youtube_title'])
            log_album(album_info, spotify_playlist_id, row['log_id'], status)
        else:
            playlist_info = find_other_playlist(row)
            if playlist_info:
                status = save_other_playlist(playlist_info, spotify_playlist_id, row['youtube_title'])
                log_other_playlist(playlist_info, spotify_playlist_id, row['log_id'], status)

    # TRACK
    # either THRESHOLD_MS is not specified or the duration of the video is less than it
    else:
        track_info = find_track(row)
        if track_info:
            status = save_track(track_info, spotify_playlist_id, row['youtube_title'])
            log_track(track_info, spotify_playlist_id, row['log_id'], status)


def create_df_spotify_albums(spotify_albums: dict[str, tuple[str]]) -> pd.DataFrame:
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


def create_df_spotify_playlists_others(spotify_playlists_others: dict[str, tuple[str]]) -> pd.DataFrame:
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


def create_df_spotify_tracks(spotify_tracks: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify track dataframe from a track dictionary.
    """
    df_spotify_tracks = pd.DataFrame.from_dict(spotify_tracks, orient='index',
                                               columns=['album_uri',
                                                        'playlist_uri',
                                                        'track_title',
                                                        'track_artists',
                                                        'duration_ms']) \
                                                .reset_index(names='track_uri')
    return df_spotify_tracks


def create_df_spotify_log(spotify_log: list[tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify log dataframe from a log list.
    """
    df_spotify_log = pd.DataFrame(spotify_log, columns=['log_id',
                                                        'spotify_uri',
                                                        'spotify_playlist_id',
                                                        # 'uri_type',
                                                        'found_on_try',
                                                        'difference_ms',
                                                        'tracks_in_desc',
                                                        'q',
                                                        'search_type_id',
                                                        'status'])

    return df_spotify_log


def create_df_search_types() -> pd.DataFrame:
    search_types = {0: 'colons',
                    1: 'title only',
                    2: 'keyword and quotes',
                    3: 'channel name and title'}
    
    df_search_types = pd.DataFrame.from_dict(search_types, orient='index',
                                            columns=['search_type_name']) \
                                            .reset_index(names='search_type_id')
    
    return df_search_types


def create_df_spotify_playlists(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_spotify_playlists = df_playlists[['spotify_playlist_id', 'playlist_name']]

    return df_spotify_playlists


def create_df_playlist_ids(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_playlist_ids = df_playlists[['youtube_playlist_id', 'spotify_playlist_id']].reset_index(names='id')

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
    load_to_bigquery(df_spotify_playlists, 'spotify_playlists')
    print(f'spotify_playlists uploaded to BigQuery.')

    df_playlist_ids = create_df_playlist_ids(df_playlists)
    load_to_bigquery(df_playlist_ids, 'playlist_ids')
    print(f'playlist_ids uploaded to BigQuery.')

    # Populate Spotify.
    spotify_albums: dict[str, tuple[str]] = {}
    spotify_playlists_others: dict[str, tuple[str]] = {}
    spotify_tracks: dict[str, tuple[str]] = {}
    spotify_log: list[tuple[str]] = []

    df_videos.apply(populate_spotify, axis = 1)

    df_spotify_albums = create_df_spotify_albums(spotify_albums)
    load_to_bigquery(df_spotify_albums, 'spotify_albums')
    print(f'spotify_albums uploaded to BigQuery, {len(df_spotify_albums)} rows.')

    df_spotify_playlists_others = create_df_spotify_playlists_others(spotify_playlists_others)
    load_to_bigquery(df_spotify_playlists_others, 'spotify_playlists_others')
    print(f'spotify_playlists_others uploaded to BigQuery, {len(df_spotify_playlists_others)} rows.')

    df_spotify_tracks = create_df_spotify_tracks(spotify_tracks)
    load_to_bigquery(df_spotify_tracks, 'spotify_tracks')
    print(f'spotify_tracks uploaded to BigQuery, {len(df_spotify_tracks)} rows.')

    df_spotify_log = create_df_spotify_log(spotify_log)
    load_to_bigquery(df_spotify_log, 'spotify_log')
    print(f'spotify_log uploaded to BigQuery, {len(df_spotify_log)} rows.')

    # Create search types.
    df_search_types = create_df_search_types()
    load_to_bigquery(df_search_types, 'search_types')
    print(f'search_types uploaded to BigQuery.')
