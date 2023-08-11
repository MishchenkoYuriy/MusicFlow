'''
API key               An API key is a unique string that lets you access an API.
Google OAuth 2.0      OAuth 2.0 provides authenticated access to an API.
'''

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

def main():

    flow = InstalledAppFlow.from_client_secrets_file("dags/client_secrets.json", scopes=["https://www.googleapis.com/auth/youtube.readonly"])
    
    credentials = flow.run_local_server(port=4040, authorization_prompt_message='')

    youtube = build("youtube", "v3", credentials=credentials)

    request = youtube.playlists().list(
        part="snippet,contentDetails",
        maxResults=25,
        mine=True
    )
    response = request.execute()

    print(response)

if __name__ == "__main__":
    main()