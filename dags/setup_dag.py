import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

from scripts.spotify_auth import get_refresh_token


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def setup_dag():
    @task
    def set_refresh_token():
        '''
        Set or update REFRESH_TOKEN in the Airflow Variables.
        Require AUTH_CODE to be set in the Airflow Variables.
        '''
        auth_code = Variable.get('AUTH_CODE')
        refresh_token = get_refresh_token(auth_code)
        Variable.set('REFRESH_TOKEN', refresh_token)
        logging.info(f'REFRESH_TOKEN has been set in the Airflow Variables.')


    set_refresh_token()


setup_dag()
