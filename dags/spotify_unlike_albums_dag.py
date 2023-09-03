from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from spotify_unlike_albums import populate_albums_uri, unlike_albums
from spotify_auth import auth_with_refresh_token


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def spotify_unlike_albums_dag():
    @task
    def task_auth_with_refresh_token():
        sp = auth_with_refresh_token()
        return sp


    @task
    def task_populate_albums_uri(sp):
        albums_uri = populate_albums_uri(sp)
        return albums_uri


    @task
    def task_unlike_albums(sp, albums_uri):
        unlike_albums(sp, albums_uri)


    sp = task_auth_with_refresh_token()
    albums_uri = task_populate_albums_uri(sp)
    task_unlike_albums(sp, albums_uri)


spotify_unlike_albums_dag()
