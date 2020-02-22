import asyncio
from timeit import default_timer
from TokenManager import TokenManager
from io import StringIO
import base64
import pandas as pd
import aiohttp


def get_data_shape(raw):
    data_string = StringIO(base64.b64decode(raw).decode('ascii'))
    data = pd.read_csv(data_string, sep=',', low_memory=False)
    return data.shape


async def fetch(session, report_id, params):
    start_time = default_timer()
    base_url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/'
    async with session.post(base_url + report_id, params=params) as response:
        json_res = await response.json()
        data = get_data_shape(json_res['file'])

        run_time = "{:5.2f}s".format((default_timer() - start_time))
        print('Report ID: {}\nCompleted Time: {}\nData Shape: {}\n'.format(report_id, run_time, data))
        return data


async def get_data_asynchronous(event_loop, headers, params):
    report_ids = ['521', '524', '544', '549', '500']

    async with aiohttp.ClientSession(loop=event_loop,
                                     headers=headers,
                                     connector=aiohttp.TCPConnector(ssl=False)) as session:

        results = await asyncio.gather(*[fetch(session, code, params) for code in report_ids], return_exceptions=True)
        return results


if __name__ == '__main__':
    access_token = TokenManager('/creds.yml')

    request_header = {'Authorization': 'Bearer {}'.format(access_token()),
                      'Content-Type': 'x-www-form-urlencoded',
                      'Accept': 'application/json'
                      }

    request_params = {'saveAsFile': 'false', 'includeHeaders': 'true',
                      'startDate': '2020-01-01T00:00:00', 'endDate': '2020-01-31T00:00:00'}

    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(get_data_asynchronous(loop, request_header, request_params))
    print(res)
