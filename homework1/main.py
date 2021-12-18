import json
import requests
import os
import sys

from requests.exceptions import RequestException
from datetime import date
from common.config import Config


def get_token(config, header):
    try:
        auth_params = json.dumps({"username": config['username'], "password": config['password']})
        return requests.post(config['url'] + '/auth', headers=header, data=auth_params).json()['access_token']
    except RequestException: raise Exception("Error with API")

def save_response(load_date):

    config = Config("./resources/config.yaml").get_config()
    header = {"content-type": config['content_type']}
    header["Authorization"] = "JWT " + get_token(config, header)
    
    if load_date: process_date = load_date
    else: process_date=str(date.today())

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


def main():
    argv = sys.argv[1:]
    if (len(argv) == 0 or len(argv) > 2): load_date = None
    else: load_date = argv[0]
    save_response(load_date)



if __name__ == '__main__':
    main()