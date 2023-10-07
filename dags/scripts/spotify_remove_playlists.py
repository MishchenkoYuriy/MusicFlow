import logging
import os

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
    offset = 0

    playlists = sp.current_user_playlists()

    for playlist in playlists["items"]:
        playlist_ids.append(playlist["id"])

    while playlists["next"]:
        offset += 50
        playlists = sp.current_user_playlists(offset=offset)

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
