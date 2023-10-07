"""
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
"""

import logging
import os

# import re
import pickle
from datetime import datetime

import aniso8601
import pandas as pd
from dotenv import load_dotenv
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    filename="logs/youtube_elt.log",
)

logging.getLogger("googleapiclient").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


load_dotenv()

# youtube_videos: dictionary that contains unique videos
distinct_videos: dict[str, list[str]] = {}
# list that maps playlist id to video id (both are strings)
youtube_library: list[tuple[str]] = []


def get_new_oauth_credential():
    """
    Return read-only OAuth credentials and write them to `token.pickle`.
    Require the user to manually authenticate in the browser.
    """
    client_secrets_path = os.getenv("CLIENT_SECRETS_PATH")
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_path, scopes=["https://www.googleapis.com/auth/youtube.readonly"]
    )
    credentials = flow.run_local_server(port=4040, authorization_prompt_message="")

    with open("token.pickle", "wb") as f:
        pickle.dump(credentials, f)

    return credentials


def get_valid_oauth_credentials():
    """
    Return valid OAuth credentials.
    """
    credentials = None

    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    if not credentials:  # credentials not exist
        credentials = get_new_oauth_credential()
        logger.info("A refresh token has been created.")

    elif not credentials.valid:
        try:
            credentials.refresh(Request())

        except RefreshError:
            os.unlink("token.pickle")  # delete token.pickle
            logger.info(
                "The refresh token has been expired after 7 days. Please reauthorise..."
            )
            credentials = get_new_oauth_credential()
            logger.info("A new token has been generated.")

    return credentials


def extract_user_playlists(youtube) -> dict[str, list[str]]:
    """
    Extract the current user's playlists
    (i.e., those created by the user) on YouTube.

    Return a dictionary with keys as identifiers
    and values as lists with playlist info (type, title, author, year).
    """
    playlists: dict[str, list[str]] = {}
    playlist_temp: list[dict] = []

    request = youtube.playlists().list(
        part="snippet,contentDetails", maxResults=50, mine=True
    )
    response = request.execute()
    playlist_temp = response["items"]

    while response.get("nextPageToken", ""):
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            maxResults=50,
            mine=True,
            pageToken=response["nextPageToken"],
        )
        response = request.execute()
        playlist_temp.extend(response["items"])

    for item in playlist_temp:
        if "💼" not in item["snippet"]["title"]:  # remove non-music playlists
            playlists[item["id"]] = [
                "Playlist",
                item["snippet"]["title"],
                item["snippet"]["channelTitle"],
                None,
            ]

    return playlists


def extract_playlist_items(youtube, playlists: dict[str, list[str]]) -> None:
    """
    Extract items (videos) from playlists.

    Populate:
    distinct_videos: dictionary with keys as identifiers and values as
    lists with track info (title, author, description, duration_ms).
    youtube_library: list of tuples mapping playlist ids to video ids.
    """
    for playlist_id in playlists:
        response = get_playlist_items_page(youtube, playlist_id)
        populate_with_playlist_items_page(response, playlist_id)

        while response.get("nextPageToken", ""):
            response = get_playlist_items_page(
                youtube, playlist_id, response["nextPageToken"]
            )
            populate_with_playlist_items_page(response, playlist_id)

    add_duration_ms(youtube)


def extract_liked_videos(youtube) -> None:
    """
    Extract videos from `Liked videos`.

    Populate:
    distinct_videos: dictionary with keys as identifiers and values as
    lists with track info (title, author, description, duration_ms).
    youtube_library: list of tuples mapping playlist ids to video ids.
    """
    response = get_liked_videos_page(youtube)
    populate_with_liked_videos_page(response)

    while response.get("nextPageToken", ""):
        response = get_liked_videos_page(youtube, response["nextPageToken"])
        populate_with_liked_videos_page(response)


def get_playlist_items_page(youtube, playlist_id: str, nextPageToken: str = ""):
    """
    Return a requested page of the API call as a json object.
    Contains information about videos in the playlist.
    """
    if nextPageToken:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=playlist_id,
            pageToken=nextPageToken,
        )
    else:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails", maxResults=50, playlistId=playlist_id
        )

    response = request.execute()
    return response


def get_liked_videos_page(youtube, nextPageToken: str = ""):
    """
    Return a requested page of the API call as a json object.
    Contains information about liked videos.
    """
    if nextPageToken:
        request = youtube.videos().list(
            part="snippet,contentDetails",
            maxResults=50,
            myRating="like",
            pageToken=nextPageToken,
        )
    else:
        request = youtube.videos().list(
            part="snippet,contentDetails", maxResults=50, myRating="like"
        )

    response = request.execute()
    return response


def populate_with_playlist_items_page(response, playlist_id: str) -> None:
    for item in response["items"]:
        # Remove deleted and private videos
        if item["snippet"]["title"] not in ("Deleted video", "Private video"):
            distinct_videos[item["contentDetails"]["videoId"]] = [
                item["snippet"]["title"],
                item["snippet"]["videoOwnerChannelTitle"],
                item["snippet"]["description"],
            ]

            youtube_library.append((playlist_id, item["contentDetails"]["videoId"]))


def populate_with_liked_videos_page(response):
    for item in response["items"]:
        # Deleted and private videos automatically excluded by YouTube Data API
        iso8601_duration = item["contentDetails"]["duration"]
        duration_ms = int(
            aniso8601.parse_duration(iso8601_duration).total_seconds() * 1000
        )

        """
        Cleaning liked videos:
        - remove albums with cyrillic in the title or with categories
          other than 'Music', 'People & Blogs'
        - remove tracks with irrelevant categories
        """
        # if (
        #     duration_ms >= int(os.getenv("THRESHOLD_MS"))
        #     and not re.search("[а-яёА-ЯЁ]", item["snippet"]["title"])
        #     and item["snippet"]["categoryId"] in ("10", "22")
        # ) or (
        #     duration_ms < int(os.getenv("THRESHOLD_MS"))
        #     and item["snippet"]["categoryId"]
        #     in ("10", "22", "1", "19", "20", "24", "27")
        # ):
        distinct_videos[item["id"]] = [
            item["snippet"]["title"],
            item["snippet"]["channelTitle"],
            item["snippet"]["description"],
            # item['snippet']['categoryId'],
            # item['snippet'].get('tags', []),
            duration_ms,
        ]

        youtube_library.append(("LM", item["id"]))


def add_duration_ms(youtube) -> None:
    """
    Populate distinct_videos with duration in milliseconds for
    each track.
    """
    chunks = [
        list(distinct_videos)[i : i + 50] for i in range(0, len(distinct_videos), 50)
    ]

    str_chunks: list[str] = [",".join(chunk) for chunk in chunks]

    for chunk_ind, str_chunk in enumerate(str_chunks):
        request = youtube.videos().list(part="snippet,contentDetails", id=str_chunk)
        response = request.execute()

        for item_ind, item in enumerate(response["items"]):
            video_id = chunks[chunk_ind][item_ind]  # get the videoId

            iso8601_duration = item["contentDetails"]["duration"]
            duration_ms = int(
                aniso8601.parse_duration(iso8601_duration).total_seconds() * 1000
            )

            # distinct_videos[video_id].append(item['snippet']['categoryId'])
            # distinct_videos[video_id].append(item['snippet'].get('tags', []))
            distinct_videos[video_id].append(duration_ms)


def create_df_playlists(playlists: dict[str, list[str]]) -> pd.DataFrame:
    """
    Return a dataframe of playlists.
    """
    df_playlists = pd.DataFrame.from_dict(
        playlists, orient="index", columns=["type", "title", "author", "year"]
    ).reset_index(names="youtube_playlist_id")

    liked = pd.DataFrame(
        {
            "youtube_playlist_id": "LM",
            "type": "Playlist",
            "title": "Your Likes",
            "author": None,
            "year": None,
        },
        index=[0],
    )

    df_playlists = pd.concat([df_playlists, liked], ignore_index=True)

    return df_playlists


def create_df_videos(distinct_videos: dict[str, list[str]]) -> pd.DataFrame:
    """
    Return a dataframe of videos.
    """
    df_videos = pd.DataFrame.from_dict(
        distinct_videos,
        orient="index",
        columns=[
            "title",
            "author",
            "description",
            # 'category_id',
            # 'tags',
            "duration_ms",
        ],
    ).reset_index(names="video_id")
    return df_videos


def create_df_youtube_library(youtube_library: list[tuple[str]]) -> pd.DataFrame:
    """
    Return a dataframe of playlists mapped to tracks.
    """
    df_youtube_library = pd.DataFrame(
        youtube_library, columns=["youtube_playlist_id", "video_id"]
    ).reset_index(names="id")
    return df_youtube_library


def load_to_bigquery(
    df: pd.DataFrame, table_name: str, schema: list = None, method: str = "replace"
) -> None:
    """
    Upload the dataframe in Google BigQuery.
    Create, replace or append depending on the method passed.
    """
    project_id = os.getenv("PROJECT_ID")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.marts.{table_name}"

    # Create marts dataset
    dataset = bigquery.Dataset(f"{project_id}.marts")
    dataset.location = "europe-west1"
    client.create_dataset(dataset, exists_ok=True)

    if method == "replace":
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"
        )
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    elif method == "append":
        client.load_table_from_dataframe(df, table_id).result()

    else:
        raise Exception("Unexpected method")


def main():
    """
    Extract `Liked videos` and current user's playlists on YouTube
    and load them into Google BigQuery using the YouTube Data API.
    """
    begin = datetime.now()
    credentials = get_valid_oauth_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    playlists = extract_user_playlists(youtube)
    logger.info(f"{len(playlists)} playlists were extracted.")

    if playlists:
        df_playlists = create_df_playlists(playlists)
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

        extract_playlist_items(youtube, playlists)

    extract_liked_videos(youtube)

    if distinct_videos:
        df_videos = create_df_videos(distinct_videos)
        schema = [
            bigquery.SchemaField("video_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("author", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("description", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("duration_ms", bigquery.enums.SqlTypeNames.INT64),
        ]
        load_to_bigquery(df_videos, "youtube_videos", schema)
        logger.info(f"youtube_videos uploaded to BigQuery, {len(df_videos)} rows.")

    if youtube_library:
        df_youtube_library = create_df_youtube_library(youtube_library)
        schema = [
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField(
                "youtube_playlist_id", bigquery.enums.SqlTypeNames.STRING
            ),
            bigquery.SchemaField("video_id", bigquery.enums.SqlTypeNames.STRING),
        ]
        load_to_bigquery(df_youtube_library, "youtube_library", schema)
        logger.info(
            f"youtube_library uploaded to BigQuery, {len(df_youtube_library)} rows."
        )
    end = datetime.now()
    logger.info(end - begin)


if __name__ == "__main__":
    main()
