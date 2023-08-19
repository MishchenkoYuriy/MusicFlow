import spotipy
from spotipy.oauth2 import SpotifyOAuth


def remove_playlists(user_id: str) -> tuple[str, int]:
    '''
    Find and delete the user's first 50 playlists.

    Return:
        playlists['next']: url to the next 50 playlists; None if there are no more playlists.
        len(playlists['items']): number of deleted playlists.
    '''
    playlists = sp.user_playlists(user_id)
    if playlists:
        for playlist in playlists['items']:
            sp.current_user_unfollow_playlist(playlist['id'])

        return playlists['next'], len(playlists['items'])


if __name__ == '__main__':
    scope = ["user-library-modify", "playlist-modify-public", "playlist-modify-private", "playlist-read-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    user_info = sp.current_user()
    user_id = user_info['id']

    removed_total = 0
    playlists_next, removed = remove_playlists(user_id)
    removed_total += removed

    while playlists_next:
        playlists_next, removed = remove_playlists(user_id)
        removed_total += removed

    print(f'{removed_total} playlists were removed')