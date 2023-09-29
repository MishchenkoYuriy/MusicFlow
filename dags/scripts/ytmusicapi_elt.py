import logging
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from ytmusicapi import YTMusic

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    filename="logs/ytmusicapi_elt.log",
)

logging.getLogger("googleapiclient").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


def extract_playlists(yt) -> dict[str, list[str]]:
    """
    Extract everything from the Playlists tab on YouTube Music:
    - `Your likes` (auto generated playlist)
    - current user's playlists
    - other users' playlists

    Return a dictionary with keys as identifiers and
    values as lists with playlist info (type, title, author, year).
    """
    playlists: dict[str, list[str]] = {}
    response = yt.get_library_playlists(limit=None)

    for playlist in response:
        playlists[playlist["playlistId"]] = [
            "Playlist",
            playlist["title"],
            playlist.get("author")[0]["name"]
            if playlist.get("author")
            else None,  # TODO: extract all authors
            None,
        ]

    return playlists


def extract_albums_and_EPs(yt) -> (dict[str, list[str]], dict[str, str]):
    """
    Extract everything from the Albums tab on YouTube Music:
    - albums
    - EPs

    Return:
    albums: dictionary with keys as identifiers and
    values as lists with playlist info (type, title, author, year).
    album_temp: dictionary with keys as identifiers
    and values as browse ids. Used to extract album items.
    """
    albums: dict[str, list[str]] = {}
    album_temp: dict[str, str] = {}
    response = yt.get_library_albums(limit=None)

    for album in response:
        albums[album["playlistId"]] = [
            album["type"],
            album["title"],
            album["artists"][0]["name"],
            int(album["year"]),
        ]

        album_temp[album["playlistId"]] = album["browseId"]

    return albums, album_temp


def extract_playlist_items(
    yt, youtube, playlists: dict[str, list[str]], album_temp: dict[str, str]
) -> (dict[str, list[str]], list[tuple[str]]):
    """
    Extract items (tracks) from playlists, albums and EPs.

    Return:
    distinct_tracks: dictionary with keys as identifiers
    and values as lists with track info
    (type, title, author, description, duration_ms).
    youtube_library: list of tuples mapping playlist ids to track ids.
    """
    distinct_tracks: dict[str, list[str]] = {}  # youtube_videos
    youtube_library: list[tuple[str]] = []  # youtube_library

    # Populate with playlist items:
    for playlist_id in playlists:
        response = yt.get_playlist(playlist_id, limit=None)

        for p_item in response["tracks"]:
            # TODO: None in videoId
            if p_item["videoId"]:
                duration_ms = p_item.get("duration_seconds", 0) * 1000

                # Extract description for the search engine:
                # (full description only available in YouTube Data API)
                if duration_ms >= int(os.getenv("THRESHOLD_MS")):
                    description = add_description(youtube, p_item["videoId"])
                else:
                    description = ""

                distinct_tracks[p_item["videoId"]] = [
                    p_item["videoType"],
                    p_item["title"],
                    p_item["artists"][0]["name"],
                    description,
                    duration_ms,
                ]

                youtube_library.append((playlist_id, p_item["videoId"]))

    # Populate with album and EP items:
    for playlist_id, browse_id in album_temp.items():
        response = yt.get_album(browse_id)

        for p_item in response["tracks"]:
            # TODO: None in videoId
            if p_item["videoId"]:
                duration_ms = p_item.get("duration_seconds", 0) * 1000

                # Extract description for the search engine:
                # (full description only available in YouTube Data API)
                if duration_ms >= int(os.getenv("THRESHOLD_MS")):
                    description = add_description(youtube, p_item["videoId"])
                else:
                    description = ""

                distinct_tracks[p_item["videoId"]] = [
                    p_item["videoType"],
                    p_item["title"],
                    p_item["artists"][0]["name"],
                    description,
                    duration_ms,
                ]

                youtube_library.append((playlist_id, p_item["videoId"]))

    return distinct_tracks, youtube_library


def add_description(youtube, video_id: str) -> str:
    """
    Return track description by track id using YouTube Data API.
    """
    request = youtube.videos().list(part="snippet,contentDetails", id=video_id)
    response = request.execute()
    return response["items"][0]["snippet"]["description"]


def create_df_playlists(
    playlists: dict[str, list[str]], albums: dict[str, list[str]]
) -> pd.DataFrame:
    """
    Return a combined dataframe of playlists, albums and EPs.
    """
    # Make sure playlists and albums
    # are separated for extract_playlist_items:
    temp = playlists.copy()
    temp.update(albums)

    df_playlists = pd.DataFrame.from_dict(
        temp, orient="index", columns=["type", "title", "author", "year"]
    ).reset_index(names="youtube_playlist_id")
    return df_playlists


def create_df_tracks(distinct_tracks: dict[str, list[str]]) -> pd.DataFrame:
    """
    Return a dataframe of tracks.
    """
    df_tracks = pd.DataFrame.from_dict(
        distinct_tracks,
        orient="index",
        columns=[
            "type",
            "title",
            "author",
            # TODO: 'description', with Youtube Data API
            # TODO: 'tags', with get_song
            "description",
            "duration_ms",
        ],
    ).reset_index(names="video_id")
    return df_tracks


def create_df_youtube_library(youtube_library: list[tuple[str]]) -> pd.DataFrame:
    """
    Return a dataframe of playlists mapped to tracks.
    """
    df_youtube_library = pd.DataFrame(
        youtube_library, columns=["youtube_playlist_id", "video_id"]
    ).reset_index(names="id")
    return df_youtube_library


def add_duration_ms(row, yt) -> int:
    """
    Return track duration in milliseconds.
    """
    response = yt.get_song(row["video_id"])
    return (
        int(
            response["microformat"]["microformatDataRenderer"]["videoDetails"][
                "durationSeconds"
            ]
        )
        * 1000
    )


def add_track_type(row, yt) -> str:
    """
    Return track type (`MUSIC_VIDEO_TYPE_UGC` if not found).

    https://ytmusicapi.readthedocs.io/en/stable/faq.html#which-videotypes-exist-and-what-do-they-mean
    """
    response = yt.get_song(row["video_id"])
    if not response["videoDetails"].get("musicVideoType"):
        logger.warning(
            f'Track "{row["title"]}" has no type. '
            'The type has been set to "MUSIC_VIDEO_TYPE_UGC".'
        )
        return "MUSIC_VIDEO_TYPE_UGC"
    else:
        return response["videoDetails"]["musicVideoType"]


def main():
    """
    Extract your library on YouTube Music and load it into
    Google BigQuery using ytmusicapi.

    It also uses the YouTube Data API (login with API key)
    to retrieve full description of the videos.
    """
    begin = datetime.now()
    yt = YTMusic(os.getenv("YTMUSICAPI_CREDENTIALS"))

    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key)

    # Playlists:
    playlists = extract_playlists(yt)

    # Albums + EPs:
    albums, album_temp = extract_albums_and_EPs(yt)

    if playlists or albums:
        df_playlists = create_df_playlists(playlists, albums)
        schema = [
            bigquery.SchemaField(
                "youtube_playlist_id", bigquery.enums.SqlTypeNames.STRING
            ),
            bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("author", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INT64),
        ]
        load_to_bigquery(df_playlists, "youtube_playlists", schema)
        logger.info(
            f"youtube_playlists uploaded to BigQuery, {len(df_playlists)} rows."
        )

    # Tracks:
    distinct_tracks, youtube_library = extract_playlist_items(
        yt, youtube, playlists, album_temp
    )

    if distinct_tracks:
        df_tracks = create_df_tracks(distinct_tracks)
        # Fix missing values in unavailable tracks:
        filt = df_tracks["duration_ms"] == 0
        df_tracks.loc[filt, "duration_ms"] = df_tracks[filt].apply(
            add_duration_ms, axis=1, args=[yt]
        )
        filt = df_tracks["type"].isna()
        df_tracks.loc[filt, "type"] = df_tracks[filt].apply(
            add_track_type, axis=1, args=[yt]
        )

        load_to_bigquery(df_tracks, "youtube_videos")
        logger.info(f"youtube_videos uploaded to BigQuery, {len(df_tracks)} rows.")

    if youtube_library:
        df_youtube_library = create_df_youtube_library(youtube_library)
        load_to_bigquery(df_youtube_library, "youtube_library")
        logger.info(
            f"youtube_library uploaded to BigQuery, {len(df_youtube_library)} rows."
        )

    end = datetime.now()
    logger.info(end - begin)


if __name__ == "__main__":
    from youtube_elt import load_to_bigquery

    main()
