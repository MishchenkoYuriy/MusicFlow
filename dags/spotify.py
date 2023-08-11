import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'spotify_client'
}

def request_access_token():

    CLIENT_ID = Variable.get("CLIENT_ID")
    CLIENT_SECRET = Variable.get("CLIENT_SECRET")

    auth_url = 'https://accounts.spotify.com/api/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }

    auth_response = requests.post(auth_url, data=data)

    access_token = auth_response.json().get('access_token')

    return access_token


with DAG(
    dag_id='request_access_token',
    default_args=default_args,
    description='Spotify DAG',
    start_date=datetime(2023, 8, 11),
    schedule='@hourly'
) as dag:
    
    task_1 = PythonOperator(
        task_id = 's_task_python',
        python_callable=request_access_token
    )
    
    
    task_1
