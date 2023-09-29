import logging
import os
import re

try:
    from .spotify_auth import auth_with_refresh_token
except:
    from spotify_auth import auth_with_refresh_token

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def populate_playlist_ids(sp) -> list[str]:
    """
    Return a list that contains IDs of current user playlists
    and liked other users' playlists.
    """
    playlist_ids = []

    playlists = sp.current_user_playlists()
    next_playlists = playlists["next"]

    for playlist in playlists["items"]:
        playlist_ids.append(playlist["id"])

    while next_playlists:
        # extract offset from the url
        offset = re.search("\d+", re.search("offset=\d+", next_playlists)[0])[0]
        playlists = sp.current_user_playlists(offset=offset)
        next_playlists = playlists["next"]

        for playlist in playlists["items"]:
            playlist_ids.append(playlist["id"])

    return playlist_ids


def unfollow_playlists(sp, playlist_ids: list[str]) -> None:
    """
    Unfollow all playlists from the list of IDs.
    """
    for playlist_id in playlist_ids:
        sp.current_user_unfollow_playlist(playlist_id)

    logger.info(f"{len(playlist_ids)} playlists have been removed.")


def main(refresh_token):
    """
    Remove (unfollow) liked or created playlists on Spotify.
    """
    sp = auth_with_refresh_token(refresh_token)
    playlist_ids = populate_playlist_ids(sp)
    unfollow_playlists(sp, playlist_ids)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    main(os.getenv("REFRESH_TOKEN"))
