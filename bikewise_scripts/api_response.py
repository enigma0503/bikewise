import json
import requests
import os
from create import create_file


def get_response(bookmark_file, local_data_dir, ts_list, url):
    header = {
        "Cache-Control": "max-age=0, private, must-revalidate",
        "Content-Type": "application/json"
    }

    for timestamps in ts_list:
        if timestamps[3] == 0:
            parameters = {"page": 1,
                          "per_page": 10000,
                          "occurred_before": timestamps[1],
                          "occurred_after": timestamps[0]
                          }

            response = requests.get(url, headers=header, params=parameters)
            data = response.json()
            data = data["incidents"]
            create_file(bookmark_file, local_data_dir, data, timestamps)
