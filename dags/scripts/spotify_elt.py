import logging
import os
import re
from collections import defaultdict
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

# Populate Spotify:
distinct_albums: dict[str, tuple[str]] = {}  # spotify_albums
distinct_playlists_others: dict[str, tuple[str]] = {}  # spotify_playlists_others
distinct_tracks: dict[str, tuple[str]] = {}  # spotify_tracks

# temp storages:
albums_to_like: list[str] = []
playlists_to_like: list[str] = []
tracks_to_like: list[str] = []
playlist_items: dict[str, list[str]] = defaultdict(list)

# spotify_log (divided to optimise the search for entries saved during the run)
log_albums: list[tuple[str]] = []
log_playlists_others: list[tuple[str]] = []
log_tracks: list[tuple[str]] = []


def extract_your_playlists() -> pd.DataFrame:
    project_id = os.getenv("PROJECT_ID")
    your_channel_name = os.getenv("YOUR_CHANNEL_NAME")
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        youtube_playlist_id,
        title
    
    FROM `{project_id}.marts.youtube_playlists`
    WHERE author = '{your_channel_name}' or youtube_playlist_id = 'LM'
    ORDER BY title
    """

    df_playlists = client.query(sql).to_dataframe()
    return df_playlists


def extract_other_playlists() -> pd.DataFrame:
    project_id = os.getenv("PROJECT_ID")
    your_channel_name = os.getenv("YOUR_CHANNEL_NAME")
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        ap.youtube_playlist_id,
        ap.type,
        ap.title,
        ap.author,
        ap.year,
        count(av.video_id) as total_tracks,
        array_agg(lower(av.title) order by al.id) as track_titles,
        array_agg(al.id order by al.id) as log_ids,
        sum(av.duration_ms) as duration_ms
    
    FROM `{project_id}.marts.youtube_playlists` ap

    INNER JOIN `{project_id}.marts.youtube_library` al
    on ap.youtube_playlist_id = al.youtube_playlist_id

    INNER JOIN `{project_id}.marts.youtube_videos` av
    on al.video_id = av.video_id

    WHERE ap.author != '{your_channel_name}'

    GROUP BY ap.youtube_playlist_id, ap.type, ap.title, ap.author, ap.year
    """

    df_playlists_others = client.query(sql).to_dataframe()
    return df_playlists_others


def extract_videos() -> pd.DataFrame:
    """
    Extract videos from 'Your Likes' and
    playlists created by the current user.
    """
    project_id = os.getenv("PROJECT_ID")
    your_channel_name = os.getenv("YOUR_CHANNEL_NAME")
    threshold_ms = os.getenv("THRESHOLD_MS")
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        yl.id as log_id,
        yl.youtube_playlist_id,
        
        yv.title,
        yv.author,
        yv.description,
        yv.duration_ms
    
    FROM `{project_id}.marts.youtube_library` yl

    INNER JOIN `{project_id}.marts.youtube_videos` yv
    ON yl.video_id = yv.video_id

    INNER JOIN `{project_id}.marts.youtube_playlists` yp
    ON yl.youtube_playlist_id = yp.youtube_playlist_id

    WHERE (yp.author = '{your_channel_name}' or yp.youtube_playlist_id = 'LM')

    ORDER BY yl.id
    """  # WHERE yv.duration_ms < {threshold_ms}

    df_videos = client.query(sql).to_dataframe()
    return df_videos


def get_user_id(sp) -> str:
    user_info = sp.current_user()
    return user_info["id"]


def create_user_playlists_from_df(row, sp, user_id) -> str:
    """
    Create private, non-collaborative Spotify playlists from the dataframe.
    Save created playlist ids as a column in the original dataframe.

    The user playlists are created beforehand to get unique Spotify ids,
    which are used to store found Spotify URIs.
    Using playlist names to store the URIs can result in lost playlist(s)
    if there are two or more user playlists with the same name.
    """
    if row["youtube_playlist_id"] != "LM":
        playlist = sp.user_playlist_create(
            user_id, row["title"], public=False, collaborative=False
        )
        return playlist["id"]
    else:
        return "LM"


def fix_title(title: str) -> str:
    # TODO title = re.sub('&', 'and', title)

    # Remove all brackets and the content inside
    # Examples: [Full Album], (Full EP), (2021), 【Complete】
    title = re.sub(r"(\((.*?)\)|\[(.*?)\]|【(.*?)】)", "", title)

    # Drop pipes (|)
    title = re.sub("\|", "", title)

    return title


def get_user_playlist_id(row, df_playlists: pd.DataFrame) -> str:
    playlist = df_playlists[
        df_playlists["youtube_playlist_id"] == row["youtube_playlist_id"]
    ]
    return playlist.iloc[0, 2]


def find_track(row, sp) -> dict:
    step_counter = 0
    # First try, depends on whether it is a Topic Channel
    if " - Topic" in row["author"]:
        artist = re.sub(" - Topic", "", row["author"])
        artist = re.sub("'", " ", artist)

        q = f'track:{row["title"]} artist:{artist}'
        track_info, step_counter = qsearch_track(
            row, sp, q=q, search_type_id=0, step_counter=step_counter, limit=2  # limit=1
        )

    else:
        q = row["title"]
        track_info, step_counter = qsearch_track(
            row, sp, q=q, search_type_id=1, step_counter=step_counter, limit=2  # limit=1
        )

    # Second try, track + space + track name in quotes
    if not track_info:
        q = f'track "{row["title"]}"'
        track_info, step_counter = qsearch_track(
            row, sp, q=q, search_type_id=2, step_counter=step_counter, limit=2
        )

    # Third try, channel name + space + track title
    if not track_info:
        artist = re.sub(" - Topic", "", row["author"])
        q = f'{artist} {row["title"]}'
        track_info, step_counter = qsearch_track(
            row, sp, q=q, search_type_id=3, step_counter=step_counter, limit=2
        )

    if not track_info:
        logger.info(f'Track "{row["title"]}" not found on Spotify')
    return track_info


def qsearch_track(
    row, sp, q: str, search_type_id: str, step_counter: int, limit: int
) -> (dict, int):
    tracks = sp.search(q=q, limit=limit, type="track")

    for track in tracks["tracks"]["items"]:
        # Increment step_counter only if tracks are found,
        # a certain limit doesn't guarantee what tracks will be found.
        step_counter += 1
        artists, artists_in_title, track_in_title = [], 0, 0
        diff = abs(track["duration_ms"] - row["duration_ms"])

        for artist in track["artists"]:
            artists.append(artist["name"])
            if artist["name"].lower() in row["title"].lower():
                artists_in_title += 1

        if track["name"].lower() in row["title"].lower():
            track_in_title = 1

        # Difference in 5 seconds or both track name and
        # at least one artist presented in video title:
        if diff <= 5000 or (track_in_title and artists_in_title):
            logger.info(
                f'Track "{row["title"]}" found on try: {step_counter}, '
                f"difference: {round(diff / 1000)} seconds. "
            )

            return {
                "track_uri": track["uri"],
                "album_uri": track["album"]["uri"],
                "track_title": track["name"],
                "track_artists": "; ".join(artist for artist in artists),
                "duration_ms": track["duration_ms"],
                "found_on_try": step_counter,
                "difference_ms": abs(diff),
                "track_match": 1,
                "q": q,
                "search_type_id": search_type_id,
            }, step_counter

    return dict(), step_counter


def collect_track(
    track_info: dict, user_playlist_id: str, video_title: str, liked_tracks_uri: list
):
    # search with primary key
    if (track_info["track_uri"], user_playlist_id) in (
        (uri, playlist_id) for _, uri, playlist_id, *_ in log_tracks
    ):
        status = "skipped (saved during the run)"
        logger.warning(f'Track "{video_title}" skipped (saved during the run)')

    elif track_info["track_uri"] in liked_tracks_uri and user_playlist_id == "LM":
        status = "skipped (saved before the run)"
        logger.warning(f'Track "{video_title}" skipped (saved before the run)')

    else:
        status = "saved"
        if user_playlist_id != "LM":
            # Add the track to the playlist
            if track_info["track_uri"] not in playlist_items[user_playlist_id]:
                playlist_items[user_playlist_id].append(track_info["track_uri"])

        else:
            # Like the track
            tracks_to_like.append(track_info["track_uri"])

    return status


def log_track(
    track_info: dict, user_playlist_id: str, log_id: str, status: str
) -> None:
    # Add to distinct_tracks only if track_uri not in distinct_tracks or
    # playlist_uri is null (prevent losing playlist_uri):
    if (
        not distinct_tracks.get(track_info["track_uri"])
        or not distinct_tracks[track_info["track_uri"]][1]
    ):
        distinct_tracks[track_info["track_uri"]] = (
            track_info["album_uri"],
            None,
            track_info["track_title"],
            track_info["track_artists"],
            track_info["duration_ms"],
        )

    log_tracks.append(
        (
            log_id,
            track_info["track_uri"],
            user_playlist_id,
            track_info["found_on_try"],
            track_info["difference_ms"],
            track_info["track_match"],
            track_info["q"],
            track_info["search_type_id"],
            status,
        )
    )


def find_album(row, sp) -> (dict, int):
    step_counter = 0
    fixed_title = fix_title(row["title"])
    q = fixed_title
    album_info, step_counter = qsearch_album(
        row, sp, q=q, search_type_id=0, step_counter=step_counter, limit=1
    )

    if not album_info:
        q = f'album "{fixed_title}"'
        album_info, step_counter = qsearch_album(
            row, sp, q=q, search_type_id=1, step_counter=step_counter, limit=1
        )

    # Try with raw title if title was changed
    if not album_info and fixed_title != row["title"]:
        q = row["title"]
        album_info, step_counter = qsearch_album(
            row, sp, q=q, search_type_id=2, step_counter=step_counter, limit=1
        )

    return album_info, step_counter


def find_album_extended(row, sp) -> dict:
    album_info, step_counter = find_album(row, sp)

    if not album_info:
        fixed_title = fix_title(row["title"])
        q = f'{row["author"]} {fixed_title}'
        album_info, step_counter = qsearch_album(
            row, sp, q=q, search_type_id=3, step_counter=step_counter, limit=1
        )

    if not album_info and row["year"]:
        q = f'{row["author"]} {fixed_title} year:{row["year"]}'
        album_info, step_counter = qsearch_album(
            row, sp, q=q, search_type_id=4, step_counter=step_counter, limit=1
        )

    return album_info


def qsearch_album(
    row, sp, q: str, search_type_id: str, step_counter: int, limit: int
) -> dict:
    max_diff = 40000
    albums = sp.search(q=q, limit=limit, type="album")

    for album in albums["albums"]["items"]:
        # Increment step_counter only if albums are found,
        # a certain limit doesn't guarantee what albums will be found.
        step_counter += 1
        tracks_uri: list[str] = []  # track uri
        tracks_info: list[
            tuple[str, str, int]
        ] = []  # (track uri, track title, track duration ms)
        diff = row["duration_ms"]
        track_match = 0
        album_length = 0

        tracks = sp.album(album["uri"])
        for track in tracks["tracks"]["items"]:
            # Extracted with YouTube Data API ("description" exists):
            if "description" in row.index:
                if track["name"].lower() in row["description"].lower():
                    track_match += 1
            # extracted with ytmusicapi ("track_titles" exists):
            else:
                # if found track title like any track title in the YouTube album:
                if [t for t in row["track_titles"] if track["name"].lower() in t]:
                    track_match += 1

            tracks_uri.append(track["uri"])
            tracks_info.append((track["uri"], track["name"], track["duration_ms"]))

            album_length += track["duration_ms"]
            diff -= track["duration_ms"]

        total_tracks = row.get("total_tracks", len(tracks_uri))
        match_percent = (
            track_match / total_tracks
        ) * 100  # in case a albums are same with a difference in few tracks

        # Album is considered found if one of three conditions is satisfied:
        # 1. Found title like YouTube title and artist like YouTube artist
        # 2. Difference in 40 seconds
        # 3. 60%+ tracks found (only if the total number of tracks is objective)
        if (
            (
                album["name"] in row["title"]
                and album["artists"][0]["name"] in row["author"]
            )
            or (abs(diff) < max_diff)
            or (total_tracks >= 4 and match_percent >= 60)
        ):
            logger.info(
                f'Album "{row["title"]}" found on try {step_counter}, '
                f"difference: {round(diff / 1000)} seconds, "
                f"{track_match} of {total_tracks} track titles "
                f"({round(match_percent)}%) found."
            )

            return {
                "album_uri": album["uri"],
                "tracks_uri": tracks_uri,
                "tracks_info": tracks_info,
                "album_title": album["name"],
                "album_artists": "; ".join(
                    artist["name"] for artist in album["artists"]
                ),
                "duration_ms": album_length,
                "total_tracks": total_tracks,
                "found_on_try": step_counter,
                "difference_ms": abs(diff),
                "track_match": track_match,
                "q": q,
                "search_type_id": search_type_id,
            }, step_counter

    return dict(), step_counter


def collect_album(
    album_info: dict, user_playlist_id: str, video_title: str, liked_albums_uri: list
):
    # search with primary key
    if (album_info["album_uri"], user_playlist_id) in (
        (uri, playlist_id) for _, uri, playlist_id, *_ in log_albums
    ):
        status = "skipped (saved during the run)"
        logger.warning(f'Album "{video_title}" skipped (saved during the run)')

    elif album_info["album_uri"] in liked_albums_uri and user_playlist_id == "LM":
        status = "skipped (saved before the run)"
        logger.warning(f'Album "{video_title}" skipped (saved before the run)')

    else:
        status = "saved"
        if user_playlist_id != "LM":
            # Add album tracks to the playlist and also remove duplicates
            playlist_items[user_playlist_id].extend(
                uri
                for uri in album_info["tracks_uri"]
                if uri not in playlist_items[user_playlist_id]
            )

        else:
            # Like the album
            albums_to_like.append(album_info["album_uri"])

    return status


def log_album(
    album_info: dict, user_playlist_id: str, log_id: str, status: str
) -> None:
    distinct_albums[album_info["album_uri"]] = (
        album_info["album_title"],
        album_info["album_artists"],
        album_info["duration_ms"],
        album_info["total_tracks"],
    )

    for track_uri, title, duration_ms in album_info["tracks_info"]:
        # Add to distinct_tracks only if track_uri not in distinct_tracks
        # or playlist_uri is null (prevent losing playlist_uri)
        if not distinct_tracks.get(track_uri) or not distinct_tracks[track_uri][1]:
            distinct_tracks[track_uri] = (
                album_info["album_uri"],
                None,
                title,
                # Same as album artists, not always correct,
                # but we don't iterate for every artist on every track.
                album_info["album_artists"],
                duration_ms,
            )

    log_albums.append(
        (
            log_id,
            album_info["album_uri"],
            user_playlist_id,
            album_info["found_on_try"],
            album_info["difference_ms"],
            album_info["track_match"],
            album_info["q"],
            album_info["search_type_id"],
            status,
        )
    )


def find_other_playlist(row, sp) -> (dict, int):
    step_counter = 0
    fixed_title = fix_title(row["title"])
    q = fixed_title
    pl_info, step_counter = qsearch_playlist(
        row, sp, q=q, search_type_id=0, step_counter=step_counter, limit=1
    )

    if not pl_info:
        q = f'playlist "{fixed_title}"'
        pl_info, step_counter = qsearch_playlist(
            row, sp, q=q, search_type_id=1, step_counter=step_counter, limit=1
        )

    # Try with raw title if title was changed
    if not pl_info and fixed_title != row["title"]:
        q = row["title"]
        pl_info, step_counter = qsearch_playlist(
            row, sp, q=q, search_type_id=2, step_counter=step_counter, limit=1
        )

    return pl_info, step_counter


def find_other_playlist_extended(row, sp) -> dict:
    pl_info, step_counter = find_other_playlist(row, sp)

    if not pl_info:
        fixed_title = fix_title(row["title"])
        q = f'{row["author"]} {fixed_title}'
        pl_info, step_counter = qsearch_playlist(
            row, sp, q=q, search_type_id=3, step_counter=step_counter, limit=1
        )

    if not pl_info and row["year"]:
        q = f'{row["author"]} {fixed_title} year:{row["year"]}'
        pl_info, step_counter = qsearch_playlist(
            row, sp, q=q, search_type_id=4, step_counter=step_counter, limit=1
        )

    return pl_info


def qsearch_playlist(
    row, sp, q: str, search_type_id: str, step_counter: int, limit: int
) -> (dict, int):
    max_diff = 40000
    playlists = sp.search(q=q, limit=limit, type="playlist")

    for playlist in playlists["playlists"]["items"]:
        # Increment step_counter only if playlists are found,
        # a certain limit doesn't guarantee what playlists will be found.
        step_counter += 1
        tracks_uri: list[str] = []  # track uri
        tracks_info: list[
            tuple[str, str, int]
        ] = []  # (track uri, track title, track duration ms)
        diff = row["duration_ms"]
        track_match = 0
        playlist_length = 0

        tracks = sp.playlist(playlist["uri"])
        for track in tracks["tracks"]["items"]:
            artists = []
            if track.get("track", ""):
                track_title = track["track"]["name"]
                # Extracted with YouTube Data API ("description" exists):
                if "description" in row.index:
                    if track_title.lower() in row["description"].lower():
                        track_match += 1

                # extracted with ytmusicapi ("track_titles" exists):
                else:
                    # if found track title like any track title in the YouTube album:
                    if [t for t in row["track_titles"] if track_title.lower() in t]:
                        track_match += 1

                for artist in track["track"]["artists"]:
                    artists.append(artist["name"])

                tracks_uri.append(track["track"]["uri"])
                tracks_info.append(
                    (
                        track["track"]["uri"],
                        track["track"]["name"],
                        artists,
                        track["track"]["duration_ms"],
                        track["track"]["album"]["uri"],
                    )
                )

                playlist_length += track["track"]["duration_ms"]
                diff -= track["track"]["duration_ms"]

        total_tracks = row.get("total_tracks", len(tracks_uri))
        match_percent = (
            track_match / total_tracks
        ) * 100  # in case a playlist are same with a difference in few tracks

        # Difference in 40 seconds or 60%+ tracks found in the YouTube
        # video description (only if the total number of tracks is objective)
        if (abs(diff) < max_diff) or (total_tracks >= 4 and match_percent >= 60):
            logger.info(
                f'Playlist "{row["title"]}" found on try {step_counter}, '
                f"difference: {round(diff / 1000)} seconds, "
                f"{track_match} of {total_tracks} track titles "
                f"({round(match_percent)}%) found."
            )

            return {
                "playlist_uri": playlist["uri"],
                "playlist_id": playlist["id"],
                "tracks_uri": tracks_uri,
                "tracks_info": tracks_info,
                "playlist_title": playlist["name"],
                "playlist_owner": playlist["owner"]["display_name"],
                "duration_ms": playlist_length,
                "total_tracks": total_tracks,
                "found_on_try": step_counter,
                "difference_ms": abs(diff),
                "track_match": track_match,
                "q": q,
                "search_type_id": search_type_id,
            }, step_counter

    return dict(), step_counter


def collect_other_playlist(
    playlist_info: dict, user_playlist_id: str, video_title: str
):
    # search with primary key
    if (playlist_info["playlist_uri"], user_playlist_id) not in (
        (uri, playlist_id) for _, uri, playlist_id, *_ in log_playlists_others
    ):
        status = "saved"
        if user_playlist_id != "LM":
            # Add playlist tracks to the current user playlist and
            # also remove duplicates
            playlist_items[user_playlist_id].extend(
                uri
                for uri in playlist_info["tracks_uri"]
                if uri not in playlist_items[user_playlist_id]
            )

        else:
            # Like the playlist
            playlists_to_like.append(playlist_info["playlist_id"])

    else:
        status = "skipped (saved during the run)"
        logger.warning(f'Playlist "{video_title}" skipped (saved during the run)')

    return status


def log_other_playlist(
    playlist_info: dict, user_playlist_id: str, log_id: str, status: str
) -> None:
    distinct_playlists_others[playlist_info["playlist_uri"]] = (
        playlist_info["playlist_title"],
        playlist_info["playlist_owner"],
        playlist_info["duration_ms"],
        playlist_info["total_tracks"],
    )

    for track_uri, title, artists, duration_ms, album_uri in playlist_info[
        "tracks_info"
    ]:
        distinct_tracks[track_uri] = (
            album_uri,
            playlist_info["playlist_uri"],
            title,
            "; ".join(artist for artist in artists),
            duration_ms,
        )

    log_playlists_others.append(
        (
            log_id,
            playlist_info["playlist_uri"],
            user_playlist_id,
            playlist_info["found_on_try"],
            playlist_info["difference_ms"],
            playlist_info["track_match"],
            playlist_info["q"],
            playlist_info["search_type_id"],
            status,
        )
    )


def prepare_videos(
    row, sp, df_playlists: pd.DataFrame, liked_albums_uri: list, liked_tracks_uri: list
) -> None:
    """
    Find albums, playlists and tracks on Spotify,
    collect URIs to like or add to user playlists, populate logs.
    """
    user_playlist_id = get_user_playlist_id(row, df_playlists)

    # ALBUM OR PLAYLIST
    # THRESHOLD_MS is specified and
    # the duration of the video is greater than or equal to it
    if os.getenv("THRESHOLD_MS") and row["duration_ms"] >= int(
        os.getenv("THRESHOLD_MS")
    ):
        album_info, _ = find_album(row, sp)
        if album_info:
            status = collect_album(
                album_info, user_playlist_id, row["title"], liked_albums_uri
            )
            log_album(album_info, user_playlist_id, row["log_id"], status)
        else:
            playlist_info, _ = find_other_playlist(row, sp)
            if playlist_info:
                status = collect_other_playlist(
                    playlist_info, user_playlist_id, row["title"]
                )
                log_other_playlist(
                    playlist_info, user_playlist_id, row["log_id"], status
                )

            if not playlist_info:
                logger.info(f'Album/Playlist "{row["title"]}" not found on Spotify')

    # TRACK
    # either THRESHOLD_MS is not specified or the duration of the video is less than it
    else:
        track_info = find_track(row, sp)
        if track_info:
            status = collect_track(
                track_info, user_playlist_id, row["title"], liked_tracks_uri
            )
            log_track(track_info, user_playlist_id, row["log_id"], status)


def prepare_playlists_others(row, sp, liked_albums_uri) -> None:
    """
    Find albums, EPs and playlists of other users.
    """
    # if row["type"] == "Album" or row["type"] == "EP":  # TODO: research Spotify EPs
    album_info = find_album_extended(row, sp)
    if album_info:
        status = collect_album(album_info, "LM", row["title"], liked_albums_uri)
        for log_id in row["log_ids"]:
            log_album(album_info, None, log_id, status)
            # TODO album not found

    # elif row["type"] == "Playlist":
    else:
        playlist_info = find_other_playlist_extended(row, sp)
        if playlist_info:
            status = collect_other_playlist(playlist_info, "LM", row["title"])
            for log_id in row["log_ids"]:
                log_other_playlist(playlist_info, None, log_id, status)
                # TODO playlist not found

        if not playlist_info:
            logger.info(f'Album/Playlist "{row["title"]}" not found on Spotify')


def like_albums(sp, albums_to_like: list[str]) -> None:
    """
    Like all album presented in the albums_to_like list. 50 albums per API call.
    """
    if albums_to_like:
        chunks = split_to_50size_chunks(albums_to_like)

        for uris in chunks:
            sp.current_user_saved_albums_add(uris)

        logger.info(f"{len(albums_to_like)} albums have been liked.")


def like_playlists(sp, playlists_to_like: list[str]) -> None:
    """
    Like all playlist presented in the playlists_to_like list. 1 playlist per API call.
    """
    if playlists_to_like:
        for playlist_id in playlists_to_like:
            sp.current_user_follow_playlist(playlist_id)

        logger.info(f"{len(playlists_to_like)} playlists have been liked.")


def like_tracks(sp, tracks_to_like: list[str]) -> None:
    """
    Like all album presented in the tracks_to_like list. 50 tracks per API call.
    """
    if tracks_to_like:
        chunks = split_to_50size_chunks(tracks_to_like)

        for uris in chunks:
            sp.current_user_saved_tracks_add(uris)

        logger.info(f"{len(tracks_to_like)} tracks have been liked.")


def populate_user_playlists(
    sp, playlist_items: dict[str, list[str]], df_playlists: pd.DataFrame
) -> None:
    """
    Save all URIs in the playlist_items to the user playlists.
    1 user playlist and 50 tracks per API call.
    """
    if playlist_items:
        for user_playlist_id, tracks_uri in playlist_items.items():
            chunks = split_to_50size_chunks(tracks_uri)

            for uris in chunks:
                sp.playlist_add_items(user_playlist_id, uris)  # duplicates may arise

            playlist = df_playlists[
                df_playlists["spotify_playlist_id"] == user_playlist_id
            ]
            title = playlist.iloc[0, 1]
            logger.info(
                f'Playlist "{title}" has been filled with {len(tracks_uri)} tracks.'
            )


def create_df_spotify_albums(distinct_albums: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify album dataframe from a album dictionary.
    """
    df_spotify_albums = pd.DataFrame.from_dict(
        distinct_albums,
        orient="index",
        columns=["album_title", "album_artists", "duration_ms", "total_tracks"],
    ).reset_index(names="album_uri")
    return df_spotify_albums


def create_df_spotify_playlists_others(
    distinct_playlists_others: dict[str, tuple[str]]
) -> pd.DataFrame:
    """
    Return a spotify album dataframe from a album dictionary.
    """
    df_spotify_playlists_others = pd.DataFrame.from_dict(
        distinct_playlists_others,
        orient="index",
        columns=["playlist_title", "playlist_owner", "duration_ms", "total_tracks"],
    ).reset_index(names="playlist_uri")
    return df_spotify_playlists_others


def create_df_spotify_tracks(distinct_tracks: dict[str, tuple[str]]) -> pd.DataFrame:
    """
    Return a spotify track dataframe from a track dictionary.
    """
    df_spotify_tracks = pd.DataFrame.from_dict(
        distinct_tracks,
        orient="index",
        columns=[
            "album_uri",
            "playlist_uri",
            "track_title",
            "track_artists",
            "duration_ms",
        ],
    ).reset_index(names="track_uri")
    return df_spotify_tracks


def create_df_spotify_log(
    log_albums: list[tuple[str]],
    log_playlists_others: list[tuple[str]],
    log_tracks: list[tuple[str]],
) -> pd.DataFrame:
    """
    Return a spotify log dataframe from log lists.
    """
    cols = [
        "log_id",
        "album_uri",
        "user_playlist_id",
        "found_on_try",
        "difference_ms",
        "track_match",
        "q",
        "search_type_id",
        "status",
    ]

    df1 = pd.DataFrame(log_albums, columns=cols)

    cols[1] = "playlist_uri"
    df2 = pd.DataFrame(log_playlists_others, columns=cols)

    cols[1] = "track_uri"
    df3 = pd.DataFrame(log_tracks, columns=cols)

    df_spotify_log = pd.concat([df1, df2, df3])
    cols.insert(1, "playlist_uri")
    cols.insert(1, "album_uri")
    cols.pop(4)  # drop user_playlist_id
    df_spotify_log = df_spotify_log[cols]

    return df_spotify_log


def create_df_search_types() -> pd.DataFrame:
    search_types = {
        0: "title (fixed)",
        1: "keyword and title in quotes",
        2: "title (raw)",
        3: "author and title (fixed)",
        4: "colons (year)",
    }

    df_search_types = pd.DataFrame.from_dict(
        search_types, orient="index", columns=["search_type_name"]
    ).reset_index(names="search_type_id")

    return df_search_types


def create_df_spotify_playlists(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_spotify_playlists = df_playlists[["spotify_playlist_id", "title"]]

    return df_spotify_playlists


def create_df_playlist_ids(df_playlists: pd.DataFrame) -> pd.DataFrame:
    df_playlist_ids = df_playlists[
        ["youtube_playlist_id", "spotify_playlist_id"]
    ].reset_index(names="id")

    return df_playlist_ids


def main():
    begin = datetime.now()
    # Extract dataframes from BigQuery:
    df_playlists = extract_your_playlists()
    df_playlists_others = extract_other_playlists()
    df_videos = extract_videos()
    logger.info("Datasets have been extracted from BigQuery.")

    # Authorisation:
    from spotify_auth import auth_with_refresh_token

    # scope = ["user-library-read",
    #          "user-library-modify",
    #          "playlist-read-private",
    #          "playlist-modify-private",
    #          "playlist-modify-public"]
    # sp = auth_with_auth_manager(scope)
    sp = auth_with_refresh_token(os.getenv("REFRESH_TOKEN"))
    user_id = get_user_id(sp)

    """
    Extract liked tracks and albums to prevent "overlike".
    Without it, you will lose overliked tracks and albums by using
    spotify_unlike scripts (the added_at field will be overwritten)
    """
    liked_tracks_uri = populate_tracks_uri(sp)
    liked_albums_uri = populate_albums_uri(sp)
    logger.info("Tracks and albums URI have been collected.")

    # Create Spotify playlists:
    df_playlists["spotify_playlist_id"] = df_playlists.apply(
        create_user_playlists_from_df, axis=1, args=[sp, user_id]
    )
    logger.info(f"{len(df_playlists)} playlists have been created.")

    df_spotify_playlists = create_df_spotify_playlists(df_playlists)
    load_to_bigquery(df_spotify_playlists, "spotify_playlists")
    logger.info("spotify_playlists uploaded to BigQuery.")

    df_playlist_ids = create_df_playlist_ids(df_playlists)
    load_to_bigquery(df_playlist_ids, "playlist_ids")
    logger.info("playlist_ids uploaded to BigQuery.")

    df_videos.apply(
        prepare_videos,
        axis=1,
        args=[sp, df_playlists, liked_albums_uri, liked_tracks_uri],
    )

    df_playlists_others.apply(
        prepare_playlists_others, axis=1, args=[sp, liked_albums_uri]
    )

    like_albums(sp, albums_to_like)
    like_playlists(sp, playlists_to_like)
    like_tracks(sp, tracks_to_like)
    populate_user_playlists(sp, playlist_items, df_playlists)

    # Load to BigQuery:
    if distinct_albums:
        df_spotify_albums = create_df_spotify_albums(distinct_albums)
        load_to_bigquery(df_spotify_albums, "spotify_albums")
        logger.info(
            f"spotify_albums uploaded to BigQuery, {len(df_spotify_albums)} rows."
        )

    if distinct_playlists_others:
        df_spotify_playlists_others = create_df_spotify_playlists_others(
            distinct_playlists_others
        )
        load_to_bigquery(df_spotify_playlists_others, "spotify_playlists_others")
        logger.info(
            f"spotify_playlists_others uploaded to BigQuery, "
            f"{len(df_spotify_playlists_others)} rows."
        )

    if distinct_tracks:
        df_spotify_tracks = create_df_spotify_tracks(distinct_tracks)
        log_schema = [
            bigquery.SchemaField("track_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("album_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("playlist_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("track_title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("track_artists", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("duration_ms", bigquery.enums.SqlTypeNames.INT64),
        ]
        load_to_bigquery(df_spotify_tracks, "spotify_tracks", log_schema)
        logger.info(
            f"spotify_tracks uploaded to BigQuery, {len(df_spotify_tracks)} rows."
        )

    # Upload logs:
    if log_albums or log_playlists_others or log_tracks:
        df_spotify_log = create_df_spotify_log(
            log_albums, log_playlists_others, log_tracks
        )
        log_schema = [
            bigquery.SchemaField("log_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("album_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("playlist_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("track_uri", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("found_on_try", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("difference_ms", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("track_match", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("q", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("search_type_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING),
        ]
        load_to_bigquery(df_spotify_log, "spotify_log", log_schema)
        logger.info(f"spotify_log uploaded to BigQuery, {len(df_spotify_log)} rows.")

    # Create search types:
    df_search_types = create_df_search_types()
    load_to_bigquery(df_search_types, "search_types")
    logger.info("search_types uploaded to BigQuery.")
    end = datetime.now()
    logger.info(end - begin)


if __name__ == "__main__":
    from spotify_unlike_albums import populate_albums_uri
    from spotify_unlike_tracks import populate_tracks_uri
    from youtube_elt import load_to_bigquery, split_to_50size_chunks

    main()
