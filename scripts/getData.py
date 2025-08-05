#!../venv/bin python3
#

import requests

API_URL = 'https://disease.sh/v3/covid-19/countries'


def get_data():

    try:
        res = requests.get(API_URL)
        if res.status_code == 200:
            json = res.json()
            return json
        else:
            print('Failed to Get Data')
    except:
        print("Failed to Open Request")


if __name__ == '__main__':
    data = get_data()
    print(data)
