import re
import logging


def populate_playlist_ids() -> list:
    '''
    Return a list of playlists IDs for the current user.
    '''
    playlist_ids = []

    playlists = sp.current_user_playlists()
    next_playlists = playlists['next']

    for playlist in playlists['items']:
        playlist_ids.append(playlist['id'])

    while next_playlists:
        # extract offset from the url
        offset = re.search('\d+', re.search('offset=\d+', next_playlists)[0])[0]
        playlists = sp.current_user_playlists(offset=offset)
        next_playlists = playlists['next']
        
        for playlist in playlists['items']:
            playlist_ids.append(playlist['id'])

    return playlist_ids


def remove(playlist_ids: list) -> None:
    '''
    Remove all playlists from the list of IDs.
    '''
    for playlist_id in playlist_ids:
        sp.current_user_unfollow_playlist(playlist_id)
    
    task_logger.info(f'{len(playlist_ids)} playlists were removed')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
    task_logger = logging.getLogger("airflow.task")
    
    from spotify_auth import auth_with_auth_manager
    # scope = ["user-library-modify", "playlist-modify-public", "playlist-modify-private", "playlist-read-private"]
    # scope = ["user-library-modify", "playlist-modify-private"]
    scope = ["playlist-read-private", "playlist-modify-private", "playlist-modify-public"]
    sp = auth_with_auth_manager(scope)

    playlist_ids = populate_playlist_ids()
    remove(playlist_ids)