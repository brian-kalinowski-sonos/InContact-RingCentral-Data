import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from timeit import default_timer
import os
import pandas as pd
import requests

from TokenManager import TokenManager


def format_data(raw):
    data_string = StringIO(base64.b64decode(raw).decode('ascii'))
    data = pd.read_csv(data_string, sep=',', low_memory=False)
    return data


def get_report_data(session, header, report_id):
    start_time = default_timer()
    base_url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/{}'.format(report_id)
    request_params = {'saveAsFile': 'false', 'includeHeaders': 'true',
                      'startDate': '2020-01-01T00:00:00', 'endDate': '2020-01-31T00:00:00'}

    with session.post(base_url, headers=header, params=request_params) as response:
        data = format_data(response.json()['file'])
        run_time = "{:5.2f}s".format((default_timer() - start_time))
        print('Report ID: {}\nCompleted Time: {}\nData Shape: {}\n'.format(report_id, run_time, data.shape))
        return data.shape


async def get_report_async(header, reports):
    with ThreadPoolExecutor(max_workers=(os.cpu_count() + 4)) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, get_report_data, *(session, header, rid)) for rid in reports]
            return await asyncio.gather(*tasks)


if __name__ == '__main__':
    access_token = TokenManager('/Users/brian.kalinowski/IdeaProjects/RingCentralData/creds.yml')

    request_header = {'Authorization': 'Bearer {}'.format(access_token()),
                      'Content-Type': 'x-www-form-urlencoded',
                      'Accept': 'application/json'
                      }

    report_ids = ['521', '524', '544', '549', '500']

    event_loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_report_async(request_header, report_ids))
    results = event_loop.run_until_complete(future)
    print(results)

