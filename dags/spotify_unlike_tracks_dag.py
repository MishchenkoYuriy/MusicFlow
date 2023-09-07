from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from scripts.spotify_unlike_tracks import populate_tracks_uri, unlike_tracks
from scripts.spotify_auth import auth_with_refresh_token


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def spotify_unlike_tracks_dag():
    @task(task_id='auth_with_refresh_token')
    def task_auth_with_refresh_token(refresh_token):
        sp = auth_with_refresh_token(refresh_token)
        return sp


    @task(task_id='populate_tracks_uri')
    def task_populate_tracks_uri(sp):
        tracks_uri = populate_tracks_uri(sp)
        return tracks_uri


    @task(task_id='unlike_tracks')
    def task_unlike_tracks(sp, tracks_uri):
        unlike_tracks(sp, tracks_uri)


    refresh_token = Variable.get('REFRESH_TOKEN')
    sp = task_auth_with_refresh_token(refresh_token)
    tracks_uri = task_populate_tracks_uri(sp)
    task_unlike_tracks(sp, tracks_uri)


spotify_unlike_tracks_dag()
