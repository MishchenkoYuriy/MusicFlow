import os
import re
from dotenv import load_dotenv
from datetime import datetime

import spotipy
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()


def populate_albums_uri(sp, remove_after: datetime = None) -> list:
    '''
    Return a list of liked albums URI for the current user.
    '''
    albums_uri = []

    liked_albums = sp.current_user_saved_albums()
    next_liked = liked_albums['next']

    for album in liked_albums['items']:
        added_at = datetime.strptime(album['added_at'], '%Y-%m-%dT%H:%M:%SZ')
        # if remove_after is None or album was added after remove_after
        if not remove_after or added_at > remove_after:
            albums_uri.append(album['album']['uri'])

    while next_liked:
        # extract offset from the url
        offset = re.search('\d+', re.search('offset=\d+', next_liked)[0])[0]
        liked_albums = sp.current_user_saved_albums(offset=offset)
        next_liked = liked_albums['next']
        
        for album in liked_albums['items']:
            added_at = datetime.strptime(album['added_at'], '%Y-%m-%dT%H:%M:%SZ')
            if not remove_after or added_at > remove_after:
                albums_uri.append(album['album']['uri'])

    return albums_uri


def unlike(sp, albums_uri: list) -> None:
    '''
    Unlike all albums from the list of albums URI. Split the liked albums into 50-size chunks
    and call current_user_saved_albums_delete on each one.
    current_user_saved_albums_delete has a limit of 50 albums
    '''
    chunks = [albums_uri[i:i + 50] for i in range(0, len(albums_uri), 50)]

    for chunk in chunks:
        sp.current_user_saved_albums_delete(chunk)


if __name__ == '__main__':
    # scope = ["user-library-read", "user-library-modify"]
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    
    if os.getenv('REMOVE_AFTER'): 
        remove_after = datetime.strptime(os.getenv('REMOVE_AFTER'), '%Y-%m-%d %H:%M:%S')
        albums_uri = populate_albums_uri(sp, remove_after)

    else: # if REMOVE_AFTER is not specified, remove all liked albums
        albums_uri = populate_albums_uri(sp)

    unlike(sp, albums_uri)
    print(f'{len(albums_uri)} liked albums were removed')