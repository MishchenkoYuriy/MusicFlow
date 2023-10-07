import logging
import os
from datetime import datetime

try:
    from .spotify_auth import auth_with_refresh_token
except:
    from spotify_auth import auth_with_refresh_token

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def populate_playlist_ids(sp, remove_after=None) -> list[str]:
    """
    Return a list that contains IDs of current user playlists
    and liked other users' playlists.
    """
    playlist_ids = []
    offset = 0

    if remove_after:
        remove_after = datetime.strptime(remove_after, "%Y-%m-%d %H:%M:%S")

    playlists = sp.current_user_playlists()

    for playlist in playlists["items"]:
        # if remove_after is None, add to the list to be removed
        if not remove_after:
            playlist_ids.append(playlist["id"])

        else:
            # remove empty playlists anyway
            if playlist["tracks"]["total"] == 0:
                playlist_ids.append(playlist["id"])
            else:  # playlist is not empty
                # Take the datetime of the first track added to the playlist:
                first_track = sp.playlist(playlist["uri"])["tracks"]["items"][0]
                added_at = datetime.strptime(
                    first_track["added_at"], "%Y-%m-%dT%H:%M:%SZ"
                )
                # If the first track was added after remove_after,
                # all tracks in the playlist were added after
                if added_at > remove_after:
                    playlist_ids.append(playlist["id"])

    while playlists["next"]:
        offset += 50
        playlists = sp.current_user_playlists(offset=offset)

        for playlist in playlists["items"]:
            if not remove_after:
                playlist_ids.append(playlist["id"])
            else:
                if playlist["tracks"]["total"] == 0:
                    playlist_ids.append(playlist["id"])
                else:
                    first_track = sp.playlist(playlist["uri"])["tracks"]["items"][0]
                    added_at = datetime.strptime(
                        first_track["added_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    if added_at > remove_after:
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
    if not os.getenv("REMOVE_AFTER"):
        logger.warning("REMOVE_AFTER is not defined, removing all playlists...")
    playlist_ids = populate_playlist_ids(sp, os.getenv("REMOVE_AFTER"))
    unfollow_playlists(sp, playlist_ids)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    main(os.getenv("REFRESH_TOKEN"))
