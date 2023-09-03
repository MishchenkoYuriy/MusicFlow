import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from spotify_unlike_albums import populate_albums_uri, unlike
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
        if os.getenv('REMOVE_AFTER'): 
            remove_after = datetime.strptime(os.getenv('REMOVE_AFTER'), '%Y-%m-%d %H:%M:%S')
            albums_uri = populate_albums_uri(sp, remove_after)

        else: # if REMOVE_AFTER is not specified, remove all liked albums
            albums_uri = populate_albums_uri(sp)
        
        return albums_uri


    @task
    def task_unlike(sp, albums_uri):
        unlike(sp, albums_uri)
        print(f'{len(albums_uri)} liked albums were removed')


    sp = task_auth_with_refresh_token()
    albums_uri = task_populate_albums_uri(sp)
    task_unlike(sp, albums_uri)


spotify_unlike_albums_dag()
