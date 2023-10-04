## Local setup
- Clone this repo
  ```
  git clone git@github.com:MishchenkoYuriy/MusicFlow.git
  ```
- Create and activate a virtual environment
- Install dependencies
  ```
  pip install -r requirements.txt
  ```
- Rename `.env.template` to `.env`
- (Optional) Estimate the threshold based on your music, copy it to `.env` (`THRESHOLD_MS`). Learn more [here](https://github.com/MishchenkoYuriy/MusicFlow#how-does-it-find-music).
- (Optional) Copy the current time UTC+0 to `.env` (`REMOVE_AFTER`). Used to undo the flow (remove all albums, playlists and tracks created during the flow).


### Google BigQuery
- [Create a Google Cloud project](https://developers.google.com/workspace/guides/create-project) and copy your Project ID to `.env` (`PROJECT_ID`)
- Select the project
- [Create a Service Account](https://cloud.google.com/iam/docs/service-accounts-create#iam-service-accounts-create-console) for the project with the following roles: BigQuery Admin, Storage Admin, Storage Object Admin, Viewer
- [Add a json key](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) to the Service Account and copy its location to `.env` (`GOOGLE_APPLICATION_CREDENTIALS`)

### YouTube
Choose one of the following setups, depending on which API you want to use to upload your data. [Here](https://github.com/MishchenkoYuriy/MusicFlow#which-flow-to-choose) are the pros and cons of each.
#### YouTube Data API
- Copy your YouTube channel name to `.env` (`YOUR_CHANNEL_NAME`)
- Add 💼 emoji to the names of playlists you don't want to copy to Spotify, such as non-music playlists
- [Enable YouTube Data API v3](https://console.cloud.google.com/apis/library/youtube.googleapis.com)
- Configure consent screen
  - Go to https://console.cloud.google.com/apis/credentials/consent
  - Select User Type: External
  - OAuth consent screen: Enter `App name`, `User support email` and `Developer contact information` &rarr; Click `Save and Continue`
  - Scopes: Click `Save and Continue`
  - Test users: `Add users` &rarr; Enter your email &rarr; Click `Add` &rarr; `Save and Continue`
  - Summary: Click `Back to Dashboard`
- Create OAuth client ID credentials
  - Go to https://console.cloud.google.com/apis/credentials
  - Click `Create credentials` &rarr; `OAuth client ID`
  - Select `Desktop app` as an Application type
  - Click `Create`
  - Download OAuth client ID credentials and copy its location to `.env` (`CLIENT_SECRETS_PATH`)

#### ytmusicapi
- Copy your YouTube channel name to `.env` (`YOUR_CHANNEL_NAME`)
- [Create an API key](https://support.google.com/googleapi/answer/6158862) and copy it to `.env` (`YOUTUBE_API_KEY`)
- [Enable YouTube Data API v3](https://console.cloud.google.com/apis/library/youtube.googleapis.com)
- Setup ytmusicapi
  - Run
    ```
    ytmusicapi oauth
    ```
  - You will be redirected to `Connect a device to your Google Account` page. Click `Continue`
  - Select your account
  - Click `Allow`
  - You will see a message `Success! Device connected`. Go to your terminal and press ENTER
  - This will create a file `oauth.json` in the current directory
  - Copy the location to `.env` (`YTMUSICAPI_CREDENTIALS`)

### Spotify
- [Create an app on Spotify Dashboard](https://developer.spotify.com/documentation/web-api/concepts/apps)
- Copy `Client ID`, `Client Secret` and `Redirect URI` from the app settings to `.env` (`SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET`, `SPOTIPY_REDIRECT_URI`)
- Create a refresh token
  - Run
    ```
    python dags/scripts/spotify_auth.py
    ```
  - Copy the link into your browser
  - Click `Agree`
  - You will be redirected to a link with the mask `(SPOTIPY_REDIRECT_URI)?code=(AUTH_CODE)`
  - Copy AUTH_CODE to `.env` (`AUTH_CODE`)
  - Run (again)
    ```
    python dags/scripts/spotify_auth.py
    ```
  - Copy REFRESH_TOKEN to `.env` (`REFRESH_TOKEN`)

### Redis
- [Install Redis](https://redis.io/docs/getting-started/installation/)
- Start the Redis server

## You're all set!
Copy your music by running these commands:
#### If you choose YouTube Music API:
- Run
  ```
  python dags/scripts/youtube_elt.py
  ```
- You will be redirected, select your account
- Click `Continue`
- Click `Continue` again
- Close the window
- Run
  ```
  python dags/scripts/spotify_elt.py
  ```
#### If you choose ytmusicapi:
  ```
  python dags/scripts/ytmusicapi_elt.py
  python dags/scripts/spotify_elt.py
  ```

### Last tips
Don't like the result? You can easily remove all playlists, tracks and albums, specify `REMOVE_AFTER` variable in `.env` file in format `%Y-%m-%d %H:%M:%S`. If not specified, your entire Spotify library will be deleted! Caveat: `REMOVE_AFTER` doesn't affect playlists, i.e. all playlists will be removed (Spotify API doesn't support `added_at` field for playlists).
```
python dags/scripts/spotify_remove_playlists.py
python dags/scripts/spotify_unlike_albums.py
python dags/scripts/spotify_unlike_tracks.py
```

MusicFlow will cache any video it finds and assign it to a Spotify album, playlist or track. If an error occurs, you don't have to wait for MusicFlow to find the same music again. Just restart the flow.
