import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime

import pandas as pd
import redis
from dotenv import load_dotenv
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    filename="logs/spotify_elt.log",
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
        yp.youtube_playlist_id,
        yp.type,
        yp.title,
        yp.author,
        yp.year,
        count(yv.video_id) as total_tracks,
        array_agg(lower(yv.title) order by yl.id) as track_titles,
        array_agg(yl.id order by yl.id) as log_ids,
        sum(yv.duration_ms) as duration_ms
    
    FROM `{project_id}.marts.youtube_playlists` yp

    INNER JOIN `{project_id}.marts.youtube_library` yl
    on yp.youtube_playlist_id = yl.youtube_playlist_id

    INNER JOIN `{project_id}.marts.youtube_videos` yv
    on yl.video_id = yv.video_id

    WHERE yp.author != '{your_channel_name}'

    GROUP BY yp.youtube_playlist_id, yp.type, yp.title, yp.author, yp.year
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
    # threshold_ms = os.getenv("THRESHOLD_MS")
    client = bigquery.Client(project=project_id)

    sql = f"""
    SELECT
        yl.id as log_id,
        yl.youtube_playlist_id,
        yl.video_id,
        
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


def get_user_playlist_id(row, df_playlists: pd.DataFrame) -> str:
    playlist = df_playlists[
        df_playlists["youtube_playlist_id"] == row["youtube_playlist_id"]
    ]
    return playlist.iloc[0, 2]


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
    """
    Clean title for optimal search on Spotify.
    """
    # Remove all brackets and the content inside
    # Examples: [Full Album], (Full EP), (2021), 【Complete】
    new_title = re.sub(r"(\((.*?)\)|\[(.*?)\]|【(.*?)】)", "", title)
    # If there is nothing left of the title, undo the last step
    if not new_title.strip():
        new_title = title

    # Remove dashes-dividers and the content inside
    new_title = re.sub(r"( -)(.*?)(- )", " ", new_title)
    if not new_title.strip():
        new_title = title

    # Drop pipes (|)
    new_title = re.sub(r"\|", "", new_title)
    if not new_title.strip():
        new_title = title

    # Fix apostrophes
    new_title = re.sub(r"‘", "'", new_title)
    if not new_title.strip():
        new_title = title

    # Drop dashes (-) not between words, numbers, ets.
    new_title = re.sub(r"\B-\B", "", new_title)
    if not new_title.strip():
        new_title = title

    # Drop the 'OST' word
    new_title = re.sub(r"\bOST\b", " ", new_title)
    if not new_title.strip():
        new_title = title

    return new_title


def find_track(row, sp) -> dict:
    step_num = 0
    is_ost = bool(re.search(r"\bOST\b", row["title"]))
    artist = re.sub(" - Topic", "", row["author"])
    fixed_title = fix_title(row["title"])

    q = f"track:{fixed_title} artist:{artist}"
    track_info, step_num = qsearch_track(row, sp, q, 0, step_num, is_ost, limit=50)

    if not track_info:
        q = fixed_title
        track_info, step_num = qsearch_track(row, sp, q, 2, step_num, is_ost, limit=50)

    if not track_info:
        q = f'track "{fixed_title}"'
        track_info, step_num = qsearch_track(row, sp, q, 4, step_num, is_ost, limit=50)

    if not track_info:
        q = f"{artist} {fixed_title}"
        track_info, step_num = qsearch_track(row, sp, q, 6, step_num, is_ost, limit=50)

    # Try with raw title if the title was changed
    if not track_info and fixed_title != row["title"]:
        q = f'track "{row["title"]}"'
        track_info, step_num = qsearch_track(row, sp, q, 5, step_num, is_ost, limit=50)

    if not track_info and fixed_title != row["title"]:
        q = row["title"]
        track_info, step_num = qsearch_track(row, sp, q, 3, step_num, is_ost, limit=50)

    if not track_info:
        logger.info(f'Track "{row["title"]}" not found on Spotify.')
    return track_info


def qsearch_track(
    row, sp, q: str, search_type_id: str, step_num: int, is_ost: bool, limit: int
) -> (dict, int):
    tracks = sp.search(q=q, limit=limit, type="track")

    for i, track in enumerate(tracks["tracks"]["items"]):
        # Break after the first loop
        if i > 0:
            break
        # Increment step_num only if tracks are found,
        # a certain limit doesn't guarantee what tracks will be found.
        step_num += 1
        artists = []
        artists_in_title = 0
        artists_in_channel = 0
        track_in_title = 0
        if not track.get("duration_ms", 0):
            logger.warning(
                f'The found track "{row["title"]}" '
                f'by {track["artists"][0]["name"]} '
                f"don't have a duration and was skipped"
            )
            break
        diff = abs(track["duration_ms"] - row["duration_ms"])

        for artist in track["artists"]:
            artists.append(artist["name"])
            if artist["name"].lower() in row["title"].lower():
                artists_in_title += 1
            if artist["name"].lower() in row["author"].lower():
                artists_in_channel += 1

        if track["name"].lower() in row["title"].lower():
            track_in_title = 1

        # Track found if one of the following is true:
        # - a track title in the video title AND
        # (if not an OST) at least one artist in the video title or the channel name
        # - difference in 5 seconds
        if (
            track_in_title and (is_ost or (artists_in_title or artists_in_channel))
        ) or diff <= 5000:
            logger.info(
                f'Track "{row["title"]}" found on try: {step_num}, '
                f"difference: {round(diff / 1000)} seconds."
            )

            return {
                "spotify_uri": track["uri"],
                "album_uri": track["album"]["uri"],
                "track_title": track["name"],
                "track_artists": "; ".join(artist for artist in artists),
                "duration_ms": track["duration_ms"],
                "found_on_try": step_num,
                "difference_ms": abs(diff),
                "q": q,
                "search_type_id": search_type_id,
            }, step_num

    return dict(), step_num


def collect_track(
    track_info: dict, user_playlist_id: str, video_title: str, liked_tracks_uri: list
) -> str:
    # search with primary key
    if (track_info["spotify_uri"], user_playlist_id) in (
        (uri, playlist_id) for _, uri, playlist_id, *_ in log_tracks
    ):
        status = "skipped (saved during the run)"
        logger.warning(f'Track "{video_title}" skipped (saved during the run)')

    elif track_info["spotify_uri"] in liked_tracks_uri and user_playlist_id == "LM":
        status = "skipped (saved before the run)"
        logger.warning(f'Track "{video_title}" skipped (saved before the run)')

    else:
        status = "saved"
        if user_playlist_id != "LM":
            # Add the track to the playlist
            if track_info["spotify_uri"] not in playlist_items[user_playlist_id]:
                playlist_items[user_playlist_id].append(track_info["spotify_uri"])

        else:
            # Like the track
            tracks_to_like.append(track_info["spotify_uri"])

    return status


def log_track(
    track_info: dict, user_playlist_id: str, log_id: str, status: str
) -> None:
    # Add to distinct_tracks only if track_uri not in distinct_tracks or
    # playlist_uri is null (prevent losing playlist_uri):
    if (
        not distinct_tracks.get(track_info["spotify_uri"])
        or not distinct_tracks[track_info["spotify_uri"]][1]
    ):
        distinct_tracks[track_info["spotify_uri"]] = (
            track_info["album_uri"],
            None,
            track_info["track_title"],
            track_info["track_artists"],
            track_info["duration_ms"],
        )

    log_tracks.append(
        (
            log_id,
            track_info["spotify_uri"],
            user_playlist_id,
            track_info["found_on_try"],
            track_info["difference_ms"],
            1,  # pseudo track_match_cnt
            1,  # pseudo total_tracks
            track_info["q"],
            track_info["search_type_id"],
            status,
        )
    )


def find_album(row, sp) -> (dict, int):
    step_num = 0
    fixed_title = fix_title(row["title"])
    q = fixed_title
    album_info, step_num = qsearch_album(row, sp, q, 2, step_num, 1)

    if not album_info:
        q = f'album "{fixed_title}"'
        album_info, step_num = qsearch_album(row, sp, q, 4, step_num, 1)

    # Try with raw title if the title was changed
    if not album_info and fixed_title != row["title"]:
        q = row["title"]
        album_info, step_num = qsearch_album(row, sp, q, 3, step_num, 1)

    return album_info, step_num


def find_album_extended(row, sp) -> dict:
    album_info, step_num = find_album(row, sp)

    if not album_info:
        fixed_title = fix_title(row["title"])
        q = f'{row["author"]} {fixed_title}'
        album_info, step_num = qsearch_album(row, sp, q, 6, step_num, 1)

    if not album_info and not pd.isna(row["year"]):
        q = f'{row["author"]} {fixed_title} year:{row["year"]}'
        album_info, step_num = qsearch_album(row, sp, q, 1, step_num, 1)

    return album_info


def qsearch_album(
    row, sp, q: str, search_type_id: str, step_num: int, limit: int
) -> (dict, int):
    max_diff = 40000
    albums = sp.search(q=q, limit=limit, type="album")

    for album in albums["albums"]["items"]:
        # Increment step_num only if albums are found,
        # a certain limit doesn't guarantee what albums will be found.
        step_num += 1
        tracks_uri: list[str] = []
        tracks_info: list[tuple[str, str, int]] = []
        diff = row["duration_ms"]
        album_length = 0

        track_match = []
        track_match_cnt = 0
        
        if "description" not in row.index:
            # BigQuery array_agg create an array with numpy.int64 values,
            # which are not JSON serializable, map to int to fix caching:
            log_ids = list(map(int, row["log_ids"]))
            # Create a temp dictionary to get log_ids from track_titles
            d_temp = dict(zip(log_ids, row["track_titles"]))

        # Get album tracks
        offset = 0
        response = sp.album_tracks(album["uri"], limit=50, offset=0)
        tracks = response["items"]

        while response["next"]:
            offset += 50
            response = sp.album_tracks(album["uri"], limit=50, offset=offset)
            tracks.extend(response["items"])

        for track in tracks:
            # Extracted with YouTube Data API ("description" exists):
            if "description" in row.index:
                if track["name"].lower() in row["description"].lower():
                    track_match_cnt += 1
            # extracted with ytmusicapi ("track_titles" exists):
            else:
                # Match YouTube and Spotify tracks in the album by title
                try:
                    match_id = next(
                        key
                        for key, val in d_temp.items()
                        if track["name"].lower() in val
                    )
                    d_temp.pop(match_id)  # remove the found id
                    track_match.append(match_id)  # save to found ids
                    track_match_cnt += 1
                except StopIteration:  # title not found
                    pass

            tracks_uri.append(track["uri"])
            tracks_info.append((track["uri"], track["name"], track["duration_ms"]))

            album_length += track["duration_ms"]
            diff -= track["duration_ms"]

        # In case of a single album-video, we can only estimate
        # the total number as the number of tracks in the Spotify album
        total_tracks = row.get("total_tracks", len(tracks_uri))

        # In case the albums are the same with a difference in few tracks
        match_percent = (track_match_cnt / total_tracks) * 100

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
                f'Album "{row["title"]}" found on try {step_num}, '
                f"difference: {round(diff / 1000)} seconds, "
                f"{track_match_cnt} of {total_tracks} track titles "
                f"({round(match_percent)}%) found."
            )

            return (
                {
                    "spotify_uri": album["uri"],
                    "tracks_uri": tracks_uri,
                    "tracks_info": tracks_info,
                    "album_title": album["name"],
                    "album_artists": "; ".join(
                        artist["name"] for artist in album["artists"]
                    ),
                    "duration_ms": album_length,
                    "total_tracks_spotify": len(tracks_uri),  # actual number of tracks
                    "total_tracks_calc": total_tracks,  # for the search only
                    "found_on_try": step_num,
                    "difference_ms": abs(diff),
                    "track_match_cnt": track_match_cnt,
                    "track_match": track_match,
                    "q": q,
                    "search_type_id": search_type_id,
                },
                step_num,
            )

    return dict(), step_num


def collect_album(
    album_info: dict, user_playlist_id: str, video_title: str, liked_albums_uri: list
) -> str:
    # search with primary key
    if (album_info["spotify_uri"], user_playlist_id) in (
        (uri, playlist_id) for _, uri, playlist_id, *_ in log_albums
    ):
        status = "skipped (saved during the run)"
        logger.warning(f'Album "{video_title}" skipped (saved during the run)')

    elif album_info["spotify_uri"] in liked_albums_uri and user_playlist_id == "LM":
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
            albums_to_like.append(album_info["spotify_uri"])

    return status


def log_album(
    album_info: dict, user_playlist_id: str, log_id: str, status: str
) -> None:
    distinct_albums[album_info["spotify_uri"]] = (
        album_info["album_title"],
        album_info["album_artists"],
        album_info["duration_ms"],
        album_info["total_tracks_spotify"],
    )

    for track_uri, title, duration_ms in album_info["tracks_info"]:
        # Add to distinct_tracks only if track_uri not in distinct_tracks
        # or playlist_uri is null (prevent losing playlist_uri)
        if not distinct_tracks.get(track_uri) or not distinct_tracks[track_uri][1]:
            distinct_tracks[track_uri] = (
                album_info["spotify_uri"],
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
            album_info["spotify_uri"],
            user_playlist_id,
            album_info["found_on_try"],
            album_info["difference_ms"],
            album_info["track_match_cnt"],
            album_info["total_tracks_calc"],
            album_info["q"],
            album_info["search_type_id"],
            status,
        )
    )


def find_other_playlist(row, sp) -> (dict, int):
    step_num = 0
    fixed_title = fix_title(row["title"])
    q = fixed_title
    pl_info, step_num = qsearch_playlist(row, sp, q, 2, step_num, 1)

    if not pl_info:
        q = f'playlist "{fixed_title}"'
        pl_info, step_num = qsearch_playlist(row, sp, q, 4, step_num, 1)

    # Try with raw title if the title was changed
    if not pl_info and fixed_title != row["title"]:
        q = row["title"]
        pl_info, step_num = qsearch_playlist(row, sp, q, 3, step_num, 1)

    return pl_info, step_num


def find_other_playlist_extended(row, sp) -> dict:
    pl_info, step_num = find_other_playlist(row, sp)

    if not pl_info:
        fixed_title = fix_title(row["title"])
        q = f'{row["author"]} {fixed_title}'
        pl_info, step_num = qsearch_playlist(row, sp, q, 6, step_num, 1)

    if not pl_info and not pd.isna(row["year"]):
        q = f'{row["author"]} {fixed_title} year:{row["year"]}'
        pl_info, step_num = qsearch_playlist(row, sp, q, 1, step_num, 1)

    return pl_info


def qsearch_playlist(
    row, sp, q: str, search_type_id: str, step_num: int, limit: int
) -> (dict, int):
    max_diff = 40000
    playlists = sp.search(q=q, limit=limit, type="playlist")

    for playlist in playlists["playlists"]["items"]:
        # Increment step_num only if playlists are found,
        # a certain limit doesn't guarantee what playlists will be found.
        step_num += 1
        tracks_uri: list[str] = []  # track uri
        tracks_info: list[tuple[str, str, int]] = []
        diff = row["duration_ms"]
        playlist_length = 0

        track_match = []
        track_match_cnt = 0
        
        if "description" not in row.index:
            # BigQuery array_agg create an array with numpy.int64 values,
            # which are not JSON serializable, map to int to fix caching:
            log_ids = list(map(int, row["log_ids"]))
            # Create a temp dictionary to get log_ids from track_titles:
            d_temp = dict(zip(log_ids, row["track_titles"]))

        # Get playlist tracks
        offset = 0
        response = sp.playlist_items(
            playlist["uri"], limit=100, offset=0, additional_types=("track",)
        )
        tracks = response["items"]

        while response["next"]:
            offset += 100
            response = sp.playlist_items(
                playlist["uri"], limit=100, offset=offset, additional_types=("track",)
            )
            tracks.extend(response["items"])

        for track in tracks:
            artists = []
            if track.get("track", ""):
                track_title = track["track"]["name"]
                # Extracted with YouTube Data API ("description" exists):
                if "description" in row.index:
                    if track_title.lower() in row["description"].lower():
                        track_match_cnt += 1
                # extracted with ytmusicapi ("track_titles" exists):
                else:
                    # Match YouTube and Spotify tracks in the playlist by title
                    try:
                        match_id = next(
                            key
                            for key, val in d_temp.items()
                            if track_title.lower() in val
                        )
                        d_temp.pop(match_id)  # remove the found id
                        track_match.append(match_id)  # save to found ids
                        track_match_cnt += 1
                    except StopIteration:  # title not found
                        pass

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

        # In case a playlist are same with a difference in few tracks
        match_percent = (track_match_cnt / total_tracks) * 100

        # Difference in 40 seconds or 60%+ tracks found in the YouTube
        # video description (only if the total number of tracks is objective)
        if (abs(diff) < max_diff) or (total_tracks >= 4 and match_percent >= 60):
            logger.info(
                f'Playlist "{row["title"]}" found on try {step_num}, '
                f"difference: {round(diff / 1000)} seconds, "
                f"{track_match_cnt} of {total_tracks} track titles "
                f"({round(match_percent)}%) found."
            )

            return (
                {
                    "spotify_uri": playlist["uri"],
                    "playlist_id": playlist["id"],
                    "tracks_uri": tracks_uri,
                    "tracks_info": tracks_info,
                    "playlist_title": playlist["name"],
                    "playlist_owner": playlist["owner"]["display_name"],
                    "duration_ms": playlist_length,
                    "total_tracks_spotify": len(tracks_uri),  # actual number of tracks
                    "total_tracks_calc": total_tracks,  # for the search only
                    "found_on_try": step_num,
                    "difference_ms": abs(diff),
                    "track_match_cnt": track_match_cnt,
                    "track_match": track_match,
                    "q": q,
                    "search_type_id": search_type_id,
                },
                step_num,
            )

    return dict(), step_num


def collect_other_playlist(
    playlist_info: dict, user_playlist_id: str, video_title: str
) -> str:
    # search with primary key
    if (playlist_info["spotify_uri"], user_playlist_id) not in (
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
    distinct_playlists_others[playlist_info["spotify_uri"]] = (
        playlist_info["playlist_title"],
        playlist_info["playlist_owner"],
        playlist_info["duration_ms"],
        playlist_info["total_tracks_spotify"],
    )

    for track_uri, title, artists, duration_ms, album_uri in playlist_info[
        "tracks_info"
    ]:
        distinct_tracks[track_uri] = (
            album_uri,
            playlist_info["spotify_uri"],
            title,
            "; ".join(artist for artist in artists),
            duration_ms,
        )

    log_playlists_others.append(
        (
            log_id,
            playlist_info["spotify_uri"],
            user_playlist_id,
            playlist_info["found_on_try"],
            playlist_info["difference_ms"],
            playlist_info["track_match_cnt"],
            playlist_info["total_tracks_calc"],
            playlist_info["q"],
            playlist_info["search_type_id"],
            status,
        )
    )


def prepare_videos(
    row,
    sp,
    redis_client,
    df_playlists: pd.DataFrame,
    liked_albums_uri: list,
    liked_tracks_uri: list,
) -> None:
    """
    Find albums, playlists and tracks on Spotify,
    collect URIs to like or add to user playlists, populate logs.
    """
    user_playlist_id = get_user_playlist_id(row, df_playlists)

    cached_spotify = redis_client.get(row["video_id"])
    if cached_spotify:
        cached_spotify = json.loads(cached_spotify)

    # ALBUM OR PLAYLIST
    # THRESHOLD_MS is specified and
    # the duration of the video is greater than or equal to it
    if os.getenv("THRESHOLD_MS") and row["duration_ms"] >= int(
        os.getenv("THRESHOLD_MS")
    ):
        if cached_spotify and "spotify:album" in cached_spotify["spotify_uri"]:
            album_info = cached_spotify
            match_percent = (
                album_info["track_match_cnt"] / album_info["total_tracks_calc"]
            ) * 100
            logger.info(
                f'Album "{row["title"]}" found on try {album_info["found_on_try"]}, '
                f'difference: {round(album_info["difference_ms"] / 1000)} seconds, '
                f'{album_info["track_match_cnt"]} of '
                f'{album_info["total_tracks_calc"]} track titles '
                f"({round(match_percent)}%) found (from cache)."
            )
        else:
            album_info, _ = find_album(row, sp)
            if album_info:
                redis_client.set(row["video_id"], json.dumps(album_info))

        if album_info:
            status = collect_album(
                album_info, user_playlist_id, row["title"], liked_albums_uri
            )
            log_album(album_info, user_playlist_id, row["log_id"], status)

        else:
            if cached_spotify and "spotify:playlist" in cached_spotify["spotify_uri"]:
                playlist_info = cached_spotify
                match_percent = (
                    playlist_info["track_match_cnt"]
                    / playlist_info["total_tracks_calc"]
                ) * 100
                logger.info(
                    f'Playlist "{row["title"]}" found on try '
                    f'{playlist_info["found_on_try"]}, '
                    f'difference: {round(playlist_info["difference_ms"] / 1000)} '
                    f'seconds, {playlist_info["track_match_cnt"]} of '
                    f'{playlist_info["total_tracks_calc"]} track titles '
                    f"({round(match_percent)}%) found (from cache)."
                )
            else:
                playlist_info, _ = find_other_playlist(row, sp)
                if playlist_info:
                    redis_client.set(row["video_id"], json.dumps(playlist_info))

            if playlist_info:
                status = collect_other_playlist(
                    playlist_info, user_playlist_id, row["title"]
                )
                log_other_playlist(
                    playlist_info, user_playlist_id, row["log_id"], status
                )

            if not playlist_info:
                logger.info(f'Album/Playlist "{row["title"]}" not found on Spotify.')

    # TRACK
    # either THRESHOLD_MS is not specified
    # or the duration of the video is less than it
    else:
        if cached_spotify and "spotify:track" in cached_spotify["spotify_uri"]:
            track_info = cached_spotify
            logger.info(
                f'Track "{row["title"]}" found on try: {track_info["found_on_try"]}, '
                f'difference: {round(track_info["difference_ms"] / 1000)} seconds '
                f"(from cache)."
            )
        else:
            track_info = find_track(row, sp)
            if track_info:
                redis_client.set(row["video_id"], json.dumps(track_info))

        if track_info:
            status = collect_track(
                track_info, user_playlist_id, row["title"], liked_tracks_uri
            )
            log_track(track_info, user_playlist_id, row["log_id"], status)


def prepare_playlists_others(row, sp, redis_client, liked_albums_uri) -> None:
    """
    Find albums, EPs and playlists of other users.
    """
    cached_spotify = redis_client.get(row["youtube_playlist_id"])
    if cached_spotify:
        cached_spotify = json.loads(cached_spotify)

    # if row["type"] == "Album" or row["type"] == "EP":  # TODO: research Spotify EPs
    if cached_spotify and "spotify:album" in cached_spotify["spotify_uri"]:
        album_info = cached_spotify
        match_percent = (
            album_info["track_match_cnt"] / album_info["total_tracks_calc"]
        ) * 100
        logger.info(
            f'Album "{row["title"]}" found on try {album_info["found_on_try"]}, '
            f'difference: {round(album_info["difference_ms"] / 1000)} seconds, '
            f'{album_info["track_match_cnt"]} of '
            f'{album_info["total_tracks_calc"]} track titles '
            f"({round(match_percent)}%) found (from cache)."
        )
    else:
        album_info = find_album_extended(row, sp)
        if album_info:
            redis_client.set(row["youtube_playlist_id"], json.dumps(album_info))

    if album_info:
        status = collect_album(album_info, "LM", row["title"], liked_albums_uri)
        # Log the found tracks
        for log_id in album_info["track_match"]:
            log_album(album_info, None, log_id, status)
            # TODO album not found

    # elif row["type"] == "Playlist":
    else:
        if cached_spotify and "spotify:playlist" in cached_spotify["spotify_uri"]:
            playlist_info = cached_spotify
            match_percent = (
                playlist_info["track_match_cnt"] / playlist_info["total_tracks_calc"]
            ) * 100
            logger.info(
                f'Playlist "{row["title"]}" found on try '
                f'{playlist_info["found_on_try"]}, '
                f'difference: {round(playlist_info["difference_ms"] / 1000)} '
                f'seconds, {playlist_info["track_match_cnt"]} of '
                f'{playlist_info["total_tracks_calc"]} track titles '
                f"({round(match_percent)}%) found (from cache)."
            )
        else:
            playlist_info = find_other_playlist_extended(row, sp)
            if playlist_info:
                redis_client.set(row["youtube_playlist_id"], json.dumps(playlist_info))

        if playlist_info:
            status = collect_other_playlist(playlist_info, "LM", row["title"])
            # Log the found tracks
            for log_id in playlist_info["track_match"]:
                log_other_playlist(playlist_info, None, log_id, status)
                # TODO playlist not found

        if not playlist_info:
            logger.info(f'Album/Playlist "{row["title"]}" not found on Spotify.')


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
        "track_match",  # track_match_cnt
        "total_tracks",
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
        0: "colons (title and artist)",
        1: "colons (year)",
        2: "title (fixed)",
        3: "title (raw)",
        4: "keyword and title in quotes (fixed)",
        5: "keyword and title in quotes (raw)",
        6: "artist and title (fixed)",
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

    redis_client = redis.Redis(host="localhost", port=6379, db=0)

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
        args=[sp, redis_client, df_playlists, liked_albums_uri, liked_tracks_uri],
    )

    df_playlists_others.apply(
        prepare_playlists_others, axis=1, args=[sp, redis_client, liked_albums_uri]
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
            bigquery.SchemaField("total_tracks", bigquery.enums.SqlTypeNames.INT64),
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
    redis_client.flushall()
    redis_client.close()


if __name__ == "__main__":
    from spotify_unlike_albums import populate_albums_uri
    from spotify_unlike_tracks import populate_tracks_uri
    from youtube_elt import load_to_bigquery, split_to_50size_chunks

    main()
