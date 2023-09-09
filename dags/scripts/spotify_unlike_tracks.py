import os
import re
import logging
from datetime import datetime

try: from .spotify_auth import auth_with_refresh_token
except: from spotify_auth import auth_with_refresh_token

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
task_logger = logging.getLogger("airflow.task")


def populate_tracks_uri(sp) -> list:
    '''
    Return a list of liked tracks URI for the current user.
    '''
    tracks_uri = []
    remove_after = None

    if os.getenv('REMOVE_AFTER'): 
        remove_after = datetime.strptime(os.getenv('REMOVE_AFTER'), '%Y-%m-%d %H:%M:%S')
    else:
        task_logger.warning(f'REMOVE_AFTER is not definied, removing all liked tracks...')

    liked_tracks = sp.current_user_saved_tracks()
    next_liked = liked_tracks['next']

    for track in liked_tracks['items']:
        added_at = datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ')
        # if remove_after is None or track was added after remove_after
        if not remove_after or added_at > remove_after:
            tracks_uri.append(track['track']['uri'])

    while next_liked:
        # extract offset from the url
        offset = re.search('\d+', re.search('offset=\d+', next_liked)[0])[0]
        liked_tracks = sp.current_user_saved_tracks(offset=offset)
        next_liked = liked_tracks['next']
        
        for track in liked_tracks['items']:
            added_at = datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ')
            if not remove_after or added_at > remove_after:
                tracks_uri.append(track['track']['uri'])

    return tracks_uri


def unlike_tracks(sp, tracks_uri: list) -> None:
    '''
    Unlike all tracks from the list of tracks URI. Split the liked tracks into 50-size chunks
    and call current_user_saved_tracks_delete on each one.
    current_user_saved_tracks_delete has a limit of 50 tracks
    '''
    chunks = [tracks_uri[i:i + 50] for i in range(0, len(tracks_uri), 50)]

    for chunk in chunks:
        sp.current_user_saved_tracks_delete(chunk)
    
    task_logger.info(f'{len(tracks_uri)} liked songs have been removed.')


def main(refresh_token):
    sp = auth_with_refresh_token(refresh_token)
    tracks_uri = populate_tracks_uri(sp)
    unlike_tracks(sp, tracks_uri)


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    main(os.getenv('REFRESH_TOKEN'))
