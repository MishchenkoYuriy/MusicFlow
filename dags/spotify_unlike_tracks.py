import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth


def populate_unlike_it() -> list:
    '''
    Return a list of liked track URIs for the current user.
    '''
    unlike_it = []

    liked_tracks = sp.current_user_saved_tracks()
    next_liked = liked_tracks['next']

    for track in liked_tracks['items']:
        unlike_it.append(track['track']['uri'])

    while next_liked:
        # extract offset from the url
        offset = re.search('\d+', re.search('offset=\d+', next_liked)[0])[0]
        liked_tracks = sp.current_user_saved_tracks(offset=offset)
        next_liked = liked_tracks['next']
        
        for track in liked_tracks['items']:
            unlike_it.append(track['track']['uri'])

    return unlike_it


def unlike(unlike_it: list) -> None:
    '''
    Unlike all tracks from the list of URIs. Split the liked tracks into 50-size chunks
    and call current_user_saved_tracks_delete on each one.
    current_user_saved_tracks_delete has a limit of 50 tracks
    '''
    chunks = [unlike_it[i:i + 50] for i in range(0, len(unlike_it), 50)]

    for chunk in chunks:
        sp.current_user_saved_tracks_delete(chunk)


if __name__ == '__main__':
    scope = ["user-library-read", "user-library-modify"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    unlike_it = populate_unlike_it()
    unlike(unlike_it)
    print(f'{len(unlike_it)} liked songs were removed')