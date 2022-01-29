import json
import requests
import os

from requests.exceptions import RequestException
from common.config import Config


def get_token(config):
    try:
        auth_params = json.dumps({"username": config['username'], "password": config['password']})
        header = {"content-type": 'application/json'}
        auth_url = config['url'] + '/auth'
        return requests.post(url=auth_url, headers=header, data=auth_params).json()['access_token']
    except RequestException: raise Exception("Error with API while get token")

def save_response(**kwargs):

    config = Config("/home/user/airflow/dags/load-data-from-api/resources/config.yaml").get_config()
    process_date = kwargs['ds']
    token = get_token(config)
    header = {"content-type": 'application/json', "Authorization":"JWT " + token}

    path_to_output= os.path.join(config['output_directory'], process_date)
    os.makedirs(path_to_output, exist_ok=True)

    data_params = json.dumps({"date": process_date})

    try:
        response = requests.get(config['url'] + config['endpoint'], headers=header, data=data_params)
        response.raise_for_status()
        data = response.json()
        with open(os.path.join(path_to_output, config['file_name']), 'w') as json_file:
            json.dump(data, json_file)
    except RequestException: raise Exception("Error with api")
