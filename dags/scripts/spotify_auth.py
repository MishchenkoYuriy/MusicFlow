import os
import base64
import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

from dotenv import load_dotenv

load_dotenv()


def print_auth_url(scope: str = 'user-library-modify,playlist-modify-private') -> None:
    '''
    Print authentication URL. Used in the project setup to get AUTH_CODE.
    '''
    client_id = os.getenv('SPOTIPY_CLIENT_ID')
    redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')
    url = f'https://accounts.spotify.com/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri}&scope={scope}'
    print(url)


def auth_with_client_credentials():
    '''
    The Client Credentials flow is used in server-to-server authentication.
    Without user authentication and access to their information. Not used in this project.
    '''
    client_id = os.getenv('SPOTIPY_CLIENT_ID')
    client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp


def auth_with_auth_manager(scope: list = ["user-library-modify", "playlist-modify-private"]):
    '''
    The authorisation code flow includes user authentication.
    To grant the permissions, the user is redirected to the specified SPOTIPY_REDIRECT_URI
    the first time and each time the scope is changed. Used for non-Airflow setup.
    '''
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope), requests_timeout=5, retries=5)
    return sp


def auth_with_refresh_token(refresh_token):
    '''
    A user authentication that uses the refresh token to generate an access token for a session.
    Prevent user input altogether. The refresh token must be set as an Airflow Variable.
    Used for Airflow setup.
    '''
    client_id = os.getenv('SPOTIPY_CLIENT_ID')
    client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
    
    credentials = f'{client_id}:{client_secret}'
    base64_encoded = base64.b64encode(credentials.encode()).decode()
    response = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type': 'refresh_token', 'refresh_token': refresh_token},
        headers={'Authorization': 'Basic ' + base64_encoded},
    )

    response_json = response.json()
    access_token = response_json.get('access_token')
    sp = spotipy.Spotify(auth=access_token, requests_timeout=5, retries=5)
    return sp


if __name__ == "__main__":
    print_auth_url()
