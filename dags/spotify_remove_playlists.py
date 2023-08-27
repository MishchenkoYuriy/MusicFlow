import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth


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


if __name__ == '__main__':
    # scope = ["user-library-modify", "playlist-modify-public", "playlist-modify-private", "playlist-read-private"]
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    playlist_ids = populate_playlist_ids()
    remove(playlist_ids)

    print(f'{len(playlist_ids)} playlists were removed')
