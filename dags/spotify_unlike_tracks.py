import os
import re
from dotenv import load_dotenv
from datetime import datetime

import spotipy
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()


def populate_track_uris(remove_after: datetime = None) -> list:
    '''
    Return a list of liked track URIs for the current user.
    '''
    track_uris = []

    liked_tracks = sp.current_user_saved_tracks()
    next_liked = liked_tracks['next']

    for track in liked_tracks['items']:
        added_at = datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ')
        # if remove_after is None or track was added after remove_after
        if not remove_after or added_at > remove_after:
            track_uris.append(track['track']['uri'])

    while next_liked:
        # extract offset from the url
        offset = re.search('\d+', re.search('offset=\d+', next_liked)[0])[0]
        liked_tracks = sp.current_user_saved_tracks(offset=offset)
        next_liked = liked_tracks['next']
        
        for track in liked_tracks['items']:
            added_at = datetime.strptime(track['added_at'], '%Y-%m-%dT%H:%M:%SZ')
            if not remove_after or added_at > remove_after:
                track_uris.append(track['track']['uri'])

    return track_uris


def unlike(track_uris: list) -> None:
    '''
    Unlike all tracks from the list of URIs. Split the liked tracks into 50-size chunks
    and call current_user_saved_tracks_delete on each one.
    current_user_saved_tracks_delete has a limit of 50 tracks
    '''
    chunks = [track_uris[i:i + 50] for i in range(0, len(track_uris), 50)]

    for chunk in chunks:
        sp.current_user_saved_tracks_delete(chunk)


if __name__ == '__main__':
    # scope = ["user-library-read", "user-library-modify"]
    scope = ["user-library-modify", "playlist-modify-private"]
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    
    if os.getenv('REMOVE_AFTER'): 
        remove_after = datetime.strptime(os.getenv('REMOVE_AFTER'), '%Y-%m-%d %H:%M:%S')
        track_uris = populate_track_uris(remove_after)

    else: # if REMOVE_AFTER is not specified, remove all liked tracks
        track_uris = populate_track_uris()

    unlike(track_uris)
    print(f'{len(track_uris)} liked songs were removed')