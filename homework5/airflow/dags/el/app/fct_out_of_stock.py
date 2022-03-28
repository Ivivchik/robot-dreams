import json
import requests
import os

from requests.exceptions import RequestException
from common.config import Config


def get_token(config):
    try:
        auth_params = json.dumps({
            "username": config['username'],
            "password": config['password']
        })
        header = {"content-type": 'application/json'}
        auth_url = config['url'] + '/auth'
        token = requests.post(
            url=auth_url,
            headers=header,
            data=auth_params
        ).json()['access_token']
        return token
    except RequestException: raise Exception("Error with API while get token")

def save_response(**kwargs):

    path_to_config = '/home/user/airflow/dags/el/resources/config.yaml'
    config = Config(path_to_config).get_config()
    process_date = kwargs['ds']
    hdfs_hook = kwargs['hdfs_hook']
    hdfs_connection = hdfs_hook.get_conn()
    token = get_token(config)
    header = {
        "content-type": 'application/json',
        "Authorization":"JWT " + token
    }
    path_to_hdfs = os.path.join(
        '/',
        config['output_directory'],
        process_date,
        config['file_name']
    )
    data_params = json.dumps({"date": process_date})

    try:
        response = requests.get(
            config['url'] + config['endpoint'],
            headers=header, data=data_params
        )
        response.raise_for_status()
        with hdfs_connection.write(
            hdfs_path=path_to_hdfs,
            overwrite=True,
            encoding='utf-8'
            ) as json_file: json.dump(response.json(), json_file)
    except RequestException: raise Exception("Error with api")
