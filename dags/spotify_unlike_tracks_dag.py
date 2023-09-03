from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from spotify_unlike_tracks import populate_tracks_uri, unlike_tracks
from spotify_auth import auth_with_refresh_token


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def spotify_unlike_tracks_dag():
    @task
    def task_auth_with_refresh_token():
        sp = auth_with_refresh_token()
        return sp


    @task
    def task_populate_tracks_uri(sp):
        tracks_uri = populate_tracks_uri(sp)
        return tracks_uri


    @task
    def task_unlike_tracks(sp, tracks_uri):
        unlike_tracks(sp, tracks_uri)


    sp = task_auth_with_refresh_token()
    tracks_uri = task_populate_tracks_uri(sp)
    task_unlike_tracks(sp, tracks_uri)


spotify_unlike_tracks_dag()
