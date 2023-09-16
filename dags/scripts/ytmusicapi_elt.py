import logging
from datetime import datetime

import pandas as pd
from youtube_elt import load_to_bigquery
from ytmusicapi import YTMusic

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# youtube_playlists:
playlists: dict[str, list[str]] = {}
albums: dict[str, list[str]] = {}

distinct_videos: dict[str, list[str]] = {}  # youtube_videos
youtube_library: list[list[str]] = []  # youtube_library


def extract_playlists(yt) -> None:
    """
    Extract playlist info from the Playlists tab on YouTube Music.
    Included: 'Your likes', current user playlists and
    other users' playlists saved in the Library.
    """
    response = yt.get_library_playlists(limit=None)

    for playlist in response:
        playlists[playlist["playlistId"]] = [
            "Playlist",
            playlist["title"],
            playlist.get("author")[0]["name"] if playlist.get("author") else None,
            None,
        ]


def extract_albums_and_EPs(yt) -> dict[str, str]:
    """
    Extract album and EP info from the Albums tab on YouTube Music.
    """
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

    return album_temp


def populate_library(response, id: str) -> None:
    """
    Fill distinct_videos and youtube_library with tracks from
    retrieved playlists, albums and EPs.
    """
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

            youtube_library.append([id, p_item["videoId"]])


def extract_playlist_items(
    yt, playlists: dict[str, list[str]], album_temp: dict[str, str]
) -> None:
    """
    Extract tracks from retrieved playlists, albums and EPs.
    """
    # Populate with playlist items:
    for playlist_id in playlists:
        response = yt.get_playlist(playlist_id, limit=None)

        populate_library(response, playlist_id)

    # Populate with album and EP items:
    for playlist_id, browse_id in album_temp.items():
        response = yt.get_album(browse_id)

        populate_library(response, playlist_id)


def create_df_playlists(
    playlists: dict[str, list[str]], albums: dict[str, list[str]]
) -> pd.DataFrame:
    """
    Return a playlist dataframe from a playlist dictionary.
    """
    playlists.update(albums)

    df_playlists = pd.DataFrame.from_dict(
        playlists, orient="index", columns=["type", "title", "author", "year"]
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
    yt = YTMusic("oauth.json")

    # Playlists:
    extract_playlists(yt)

    # Albums + EPs:
    album_temp = extract_albums_and_EPs(yt)

    # Tracks:
    extract_playlist_items(yt, playlists, album_temp)

    df_playlists = create_df_playlists(playlists, albums)
    load_to_bigquery(df_playlists, "altyoutube_playlists")
    logger.info(f"altyoutube_playlists uploaded to BigQuery, {len(df_playlists)} rows.")

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

    df_youtube_library = create_df_youtube_library(youtube_library)
    load_to_bigquery(df_youtube_library, "altyoutube_library")
    logger.info(
        f"altyoutube_library uploaded to BigQuery, {len(df_youtube_library)} rows."
    )
    end = datetime.now()
    logger.info(end - begin)


if __name__ == "__main__":
    main()
