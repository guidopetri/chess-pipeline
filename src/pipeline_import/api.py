#! /usr/bin/env python3


def get_cloud_eval(fen, variation_count=1, variant='standard'):
    import requests
    import time

    cloud_eval_url = 'https://lichess.org/api/cloud-eval'
    params = {'fen': fen,
              'multiPv': variation_count,
              'variant': variant,
              }

    while True:
        r = requests.get(cloud_eval_url, params=params)

        if r.status_code == 429:
            time.sleep(60)
        elif r.status_code == 200:
            break

    data = r.json()

    if 'error' in data:
        return {}

    reformed_data = {'depth': data['depth'],
                     'evaluations': [pv['cp']
                                     for pv in data['pvs']],
                     }

    return reformed_data
