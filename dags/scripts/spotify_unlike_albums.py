import logging
import os
import re
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


def populate_albums_uri(sp) -> list:
    """
    Return a list of liked albums URI for the current user.
    """
    albums_uri = []
    remove_after = None

    if os.getenv("REMOVE_AFTER"):
        remove_after = datetime.strptime(os.getenv("REMOVE_AFTER"), "%Y-%m-%d %H:%M:%S")
    else:
        logger.warning("REMOVE_AFTER is not defined, removing all liked albums...")

    liked_albums = sp.current_user_saved_albums()
    next_liked = liked_albums["next"]

    for album in liked_albums["items"]:
        added_at = datetime.strptime(album["added_at"], "%Y-%m-%dT%H:%M:%SZ")
        # if remove_after is None or album was added after remove_after
        if not remove_after or added_at > remove_after:
            albums_uri.append(album["album"]["uri"])

    while next_liked:
        # extract offset from the url
        offset = re.search("\d+", re.search("offset=\d+", next_liked)[0])[0]
        liked_albums = sp.current_user_saved_albums(offset=offset)
        next_liked = liked_albums["next"]

        for album in liked_albums["items"]:
            added_at = datetime.strptime(album["added_at"], "%Y-%m-%dT%H:%M:%SZ")
            if not remove_after or added_at > remove_after:
                albums_uri.append(album["album"]["uri"])

    return albums_uri


def unlike_albums(sp, albums_uri: list) -> None:
    """
    Unlike all albums from the list of albums URI.
    Split the liked albums into 50-size chunks
    and call current_user_saved_albums_delete on each one.
    current_user_saved_albums_delete has a limit of 50 albums
    """
    chunks = [albums_uri[i : i + 50] for i in range(0, len(albums_uri), 50)]

    for chunk in chunks:
        sp.current_user_saved_albums_delete(chunk)

    logger.info(f"{len(albums_uri)} liked albums have been removed.")


def main(refresh_token):
    sp = auth_with_refresh_token(refresh_token)
    albums_uri = populate_albums_uri(sp)
    unlike_albums(sp, albums_uri)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    main(os.getenv("REFRESH_TOKEN"))
