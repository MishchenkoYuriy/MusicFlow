import logging
import os
from datetime import datetime

import pandas as pd
from ytmusicapi import YTMusic

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def extract_playlists(yt) -> dict[str, list[str]]:
    """
    Extract playlist info from the Playlists tab on YouTube Music.
    Included: 'Your likes', current user playlists and
    other users' playlists saved in the Library.
    """
    playlists: dict[str, list[str]] = {}
    response = yt.get_library_playlists(limit=None)

    for playlist in response:
        playlists[playlist["playlistId"]] = [
            "Playlist",
            playlist["title"],
            playlist.get("author")[0]["name"] if playlist.get("author") else None,
            None,
        ]

    return playlists


def extract_albums_and_EPs(yt) -> (dict[str, list[str]], dict[str, str]):
    """
    Extract album and EP info from the Albums tab on YouTube Music.
    """
    albums: dict[str, list[str]] = {}
    album_temp: dict[str, str] = {}
    response = yt.get_library_albums(limit=None)

    for album in response:
        albums[album["playlistId"]] = [
            album["type"],
            album["title"],
            album["artists"][0]["name"],
            album["year"],
        ]

        album_temp[album["playlistId"]] = album["browseId"]

    return albums, album_temp


def extract_playlist_items(
    yt, playlists: dict[str, list[str]], album_temp: dict[str, str]
) -> (dict[str, list[str]], list[list[str]]):
    """
    Extract tracks from retrieved playlists, albums and EPs.
    """
    distinct_videos: dict[str, list[str]] = {}  # youtube_videos
    youtube_library: list[list[str]] = []  # youtube_library

    # Populate with playlist items:
    for playlist_id in playlists:
        response = yt.get_playlist(playlist_id, limit=None)

        # populate_library(response, playlist_id)
        for p_item in response["tracks"]:
            # TODO: None in videoId
            if p_item["videoId"]:
                distinct_videos[p_item["videoId"]] = [
                    p_item["videoType"],
                    p_item["title"],
                    p_item["artists"][0]["name"],
                    p_item.get("duration_seconds") * 1000
                    if p_item.get("duration_seconds")
                    else 0,
                ]

                youtube_library.append([playlist_id, p_item["videoId"]])

    # Populate with album and EP items:
    for playlist_id, browse_id in album_temp.items():
        response = yt.get_album(browse_id)

        # populate_library(response, playlist_id)
        for p_item in response["tracks"]:
            # TODO: None in videoId
            if p_item["videoId"]:
                distinct_videos[p_item["videoId"]] = [
                    p_item["videoType"],
                    p_item["title"],
                    p_item["artists"][0]["name"],
                    p_item.get("duration_seconds") * 1000
                    if p_item.get("duration_seconds")
                    else 0,
                ]

                youtube_library.append([playlist_id, p_item["videoId"]])

    return distinct_videos, youtube_library


def create_df_playlists(
    playlists: dict[str, list[str]], albums: dict[str, list[str]]
) -> pd.DataFrame:
    """
    Return a playlist dataframe from a playlist dictionary.
    """
    # Make sure playlists and albums
    # are separated for extract_playlist_items:
    temp = playlists.copy()
    temp.update(albums)

    df_playlists = pd.DataFrame.from_dict(
        temp, orient="index", columns=["type", "title", "author", "year"]
    ).reset_index(names="youtube_playlist_id")
    return df_playlists


def create_df_videos(distinct_videos: dict[str, list[str]]) -> pd.DataFrame:
    """
    Return a video dataframe from a video dictionary.
    """
    df_videos = pd.DataFrame.from_dict(
        distinct_videos,
        orient="index",
        columns=[
            "type",
            "title",
            "author",
            # TODO: 'description', with Youtube Data API
            # TODO: 'tags', with get_song
            "duration_ms",
        ],
    ).reset_index(names="video_id")
    return df_videos


def create_df_youtube_library(youtube_library: list[list[str]]) -> pd.DataFrame:
    """
    Return a library dataframe from a library list.
    """
    df_youtube_library = pd.DataFrame(
        youtube_library, columns=["youtube_playlist_id", "video_id"]
    ).reset_index(names="id")
    return df_youtube_library


def add_duration_ms(row, yt) -> int:
    """
    Return duration in milliseconds from the get_song response.
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


def add_video_type(row, yt) -> str:
    """
    Return video type from the get_song response.
    """
    response = yt.get_song(row["video_id"])
    if not response["videoDetails"].get("musicVideoType"):
        logger.warning(
            f'Video "{row["title"]}" has no type. '
            'The type has been set to "MUSIC_VIDEO_TYPE_UGC".'
        )
        return "MUSIC_VIDEO_TYPE_UGC"
    else:
        return response["videoDetails"]["musicVideoType"]


def main():
    begin = datetime.now()
    yt = YTMusic(os.getenv("YTMUSICAPI_CREDENTIALS"))

    # Playlists:
    playlists = extract_playlists(yt)

    # Albums + EPs:
    albums, album_temp = extract_albums_and_EPs(yt)

    if playlists or albums:
        df_playlists = create_df_playlists(playlists, albums)
        load_to_bigquery(df_playlists, "altyoutube_playlists")
        logger.info(
            f"altyoutube_playlists uploaded to BigQuery, {len(df_playlists)} rows."
        )

    # Tracks:
    distinct_videos, youtube_library = extract_playlist_items(yt, playlists, album_temp)

    if distinct_videos:
        df_videos = create_df_videos(distinct_videos)
        # Fix missing values in unavailable videos:
        filt = df_videos["duration_ms"] == 0
        df_videos.loc[filt, "duration_ms"] = df_videos[filt].apply(
            add_duration_ms, axis=1, args=[yt]
        )
        filt = df_videos["type"].isna()
        df_videos.loc[filt, "type"] = df_videos[filt].apply(
            add_video_type, axis=1, args=[yt]
        )

        load_to_bigquery(df_videos, "altyoutube_videos")
        logger.info(f"altyoutube_videos uploaded to BigQuery, {len(df_videos)} rows.")

    if youtube_library:
        df_youtube_library = create_df_youtube_library(youtube_library)
        load_to_bigquery(df_youtube_library, "altyoutube_library")
        logger.info(
            f"altyoutube_library uploaded to BigQuery, {len(df_youtube_library)} rows."
        )

    end = datetime.now()
    logger.info(end - begin)


if __name__ == "__main__":
    from youtube_elt import load_to_bigquery

    main()
