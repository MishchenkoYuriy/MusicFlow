from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from spotify_unlike_albums import populate_albums_uri, unlike_albums
from spotify_auth import auth_with_refresh_token


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def spotify_unlike_albums_dag():
    @task(task_id='auth_with_refresh_token')
    def task_auth_with_refresh_token(refresh_token):
        sp = auth_with_refresh_token(refresh_token)
        return sp


    @task(task_id='populate_albums_uri')
    def task_populate_albums_uri(sp):
        albums_uri = populate_albums_uri(sp)
        return albums_uri


    @task(task_id='unlike_albums')
    def task_unlike_albums(sp, albums_uri):
        unlike_albums(sp, albums_uri)


    refresh_token = Variable.get('REFRESH_TOKEN')
    sp = task_auth_with_refresh_token(refresh_token)
    albums_uri = task_populate_albums_uri(sp)
    task_unlike_albums(sp, albums_uri)


spotify_unlike_albums_dag()
