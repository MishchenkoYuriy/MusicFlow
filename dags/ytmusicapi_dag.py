import logging
import os
from datetime import datetime

from airflow.decorators import dag, task
from google.cloud import bigquery
from scripts.youtube_elt import load_to_bigquery
from scripts.ytmusicapi_elt import (
    add_duration_ms,
    add_video_type,
    create_df_playlists,
    create_df_videos,
    create_df_youtube_library,
    extract_albums_and_EPs,
    extract_playlist_items,
    extract_playlists,
)
from ytmusicapi import YTMusic

logger = logging.getLogger(__name__)


def extract_playlists_from_bq() -> dict[str, None]:
    """
    Return temp playlists from BigQuery table.
    """
    playlist_temp: dict[str, None] = {}

    project_id = os.getenv("PROJECT_ID")
    client = bigquery.Client(project=project_id)
    sql = f"""
    SELECT youtube_playlist_id
    FROM `{project_id}.marts.altyoutube_playlists`
    where type = 'Playlist'
    """

    query_job = client.query(sql)
    playlist_temp = {row.youtube_playlist_id: None for row in query_job}
    return playlist_temp


@dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
def ytmusicapi_dag():
    @task
    def altyoutube_playlists():
        yt = YTMusic(os.getenv("YTMUSICAPI_CREDENTIALS"))

        playlists = extract_playlists(yt)
        albums, album_temp = extract_albums_and_EPs(yt)

        df_playlists = create_df_playlists(playlists, albums)
        load_to_bigquery(df_playlists, "altyoutube_playlists")
        logger.info(
            f"altyoutube_playlists uploaded to BigQuery, {len(df_playlists)} rows."
        )
        return album_temp

    @task
    def altyoutube_videos(album_temp: dict[str, str]):
        yt = YTMusic(os.getenv("YTMUSICAPI_CREDENTIALS"))

        playlists = extract_playlists_from_bq()
        # TODO: check len of album_temp and len of altyoutube_playlists
        # where type in ('album', 'EP')
        distinct_videos, youtube_library = extract_playlist_items(
            yt, playlists, album_temp
        )

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
            logger.info(
                f"altyoutube_videos uploaded to BigQuery, {len(df_videos)} rows."
            )

        if youtube_library:
            df_yt_library = create_df_youtube_library(youtube_library)
            load_to_bigquery(df_yt_library, "altyoutube_library")
            logger.info(
                f"altyoutube_library uploaded to BigQuery, {len(df_yt_library)} rows."
            )

    album_temp = altyoutube_playlists()
    altyoutube_videos(album_temp)


ytmusicapi_dag()
