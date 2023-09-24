"""
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
"""

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

load_dotenv()

# youtube_videos: dictionary that contains unique videos
distinct_videos: dict[str, list[str]] = {}
# list that maps playlist id to video id (both are strings)
youtube_library: list[list[str]] = []


def get_new_credentials():
    """
    Create the read-only OAuth credentials from the
    `client_secrets.json`. Return and write the credentials to
    `token.pickle`. Redirection is required.
    """
    client_secrets_path = os.getenv("CLIENT_SECRETS_PATH")
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_path, scopes=["https://www.googleapis.com/auth/youtube.readonly"]
    )
    credentials = flow.run_local_server(port=4040, authorization_prompt_message="")

    with open("token.pickle", "wb") as f:
        pickle.dump(credentials, f)

    return credentials


def get_valid_credentials():
    """
    Return the valid credentials.
    """
    credentials = None

    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    if not credentials:  # credentials not exist
        credentials = get_new_credentials()
        print("A refresh token has been created.")

    elif not credentials.valid:
        try:
            credentials.refresh(Request())

        except RefreshError:
            os.unlink("token.pickle")  # delete token.pickle
            print(
                "The refresh token has been expired after 7 days. Please reauthorise..."
            )
            credentials = get_new_credentials()
            print("A new token has been generated.")

    return credentials


def extract_user_playlists(youtube) -> dict[str, list[str]]:
    """
    Return a dictionary of music playlists created by the current user.
    """
    request = youtube.playlists().list(
        part="snippet,contentDetails", maxResults=50, mine=True
    )
    response = request.execute()

    playlists: dict[str, list[str]] = {}

    for item in response["items"]:
        if "💼" not in item["snippet"]["title"]:  # remove non-music playlists
            playlists[item["id"]] = [
                "Playlist",
                item["snippet"]["title"],
                item["snippet"]["channelTitle"],
                None,
            ]

    return playlists


def extract_videos(youtube, playlists: dict[str, list[str]]) -> None:
    """
    Extract videos from retrieved playlists and fill distinct_videos
    and youtube_library with them.
    """
    extract_videos_from_playlists(youtube, playlists)
    extract_liked_videos(youtube)


def extract_videos_from_playlists(youtube, playlists: dict[str, list[str]]) -> None:
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

            youtube_library.append([playlist_id, item["contentDetails"]["videoId"]])


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

        youtube_library.append(["LM", item["id"]])


def split_to_50size_chunks(distinct_videos: dict[str, list[str]]) -> list[list[str]]:
    """
    Split video ids into 50-size chunks.
    """
    chunks: list[list[str]] = []
    chunk: list[str] = []

    for ind, video_id in enumerate(distinct_videos):
        if ind % 50 == 0 and chunk:  # not include the first empty chunk
            chunks.append(chunk)
            chunk = []
        chunk.append(video_id)

    chunks.append(chunk)  # append the last chunk
    return chunks


def add_duration_ms(youtube) -> None:
    """
    Call videos().list for each chunk in the chunks list.
    Extract duration, convert it to milliseconds and store it
    in the distinct_videos.
    """
    chunks = split_to_50size_chunks(distinct_videos)

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
    Return a playlist dataframe from a playlist dictionary.
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
    Return a video dataframe from a video dictionary.
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


def create_df_youtube_library(youtube_library: list[list[str]]) -> pd.DataFrame:
    """
    Return a library dataframe from a youtube_library list.
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
    begin = datetime.now()
    credentials = get_valid_credentials()
    youtube = build("youtube", "v3", credentials=credentials)

    playlists = extract_user_playlists(youtube)
    print(f"{len(playlists)} playlists were extracted.")

    df_playlists = create_df_playlists(playlists)
    load_to_bigquery(df_playlists, "youtube_playlists")
    print(f"youtube_playlists uploaded to BigQuery, {len(df_playlists)} rows.")

    extract_videos(youtube, playlists)

    df_videos = create_df_videos(distinct_videos)
    load_to_bigquery(df_videos, "youtube_videos")
    print(f"youtube_videos uploaded to BigQuery, {len(df_videos)} rows.")

    df_youtube_library = create_df_youtube_library(youtube_library)
    load_to_bigquery(df_youtube_library, "youtube_library")
    print(f"youtube_library uploaded to BigQuery, {len(df_youtube_library)} rows.")
    end = datetime.now()
    print(end - begin)


if __name__ == "__main__":
    main()
