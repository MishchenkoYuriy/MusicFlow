import os
import base64
import requests
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago


@dag(
    start_date = days_ago(1),
    schedule=None,
    catchup=False
)
def setup_dag():
    @task
    def set_refresh_token() -> None:
        '''
        Set or update REFRESH_TOKEN in the Airflow Variables.
        Require AUTH_CODE to be set in the Airflow Variables.
        '''
        client_id = os.getenv('SPOTIPY_CLIENT_ID')
        client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
        redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')
        auth_code = Variable.get('AUTH_CODE')

        credentials = f'{client_id}:{client_secret}'
        base64_encoded = base64.b64encode(credentials.encode()).decode()
        response = requests.post(
            'https://accounts.spotify.com/api/token',
            data={"grant_type": "authorization_code", "code": auth_code, "redirect_uri": redirect_uri},
            headers={"Authorization": "Basic " + base64_encoded},
        )

        response_json = response.json()

        if not response_json.get("refresh_token"):
            raise ValueError('REFRESH_TOKEN is null, try to set the AUTH_CODE again.')

        else:
            # logging.info(f'ACCESS_TOKEN={response_json.get("access_token")}')
            logging.info(f'REFRESH_TOKEN={response_json.get("refresh_token")}')

            Variable.set('REFRESH_TOKEN', response_json.get('refresh_token'))
            logging.info(f'REFRESH_TOKEN has been set in the Airflow Variables.')


    set_refresh_token()


setup_dag()
