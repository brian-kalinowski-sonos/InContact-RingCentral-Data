import asyncio
import base64
from asyncio.events import AbstractEventLoop
from io import StringIO
from timeit import default_timer
from typing import *

import aiohttp
import pandas as pd
from aiohttp import ClientSession
from dateutil.parser import parse

from TokenManager import TokenManager


class HistoricalReport:
    def __init__(self, start_date: str, end_date: str):
        # TODO change this for service account
        self.token = TokenManager('/Users/brian.kalinowski/IdeaProjects/RingCentralData/creds.yml')

        # gets 30 day intervals of dates from start till end
        self.date_batches = self.get_date_range(start_date, end_date)

        # Default API headers
        self.request_headers = {'Authorization': 'Bearer {}'.format(self.token()),
                                'Content-Type': 'x-www-form-urlencoded',
                                'Accept': 'application/json'
                                }

        # holds the df for each batch in the date range
        self.report_df_batches = []

    async def run_report(self, session: ClientSession, param: dict, report_id: str) -> pd.DataFrame:
        start_time = default_timer()
        url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/{}'.format(report_id)
        async with session.post(url, params=param) as response:
            response.raise_for_status()
            json_result = await response.json()
            data = self.format_data(json_result['file'])
            run_time = '{:5.2f}s'.format((default_timer() - start_time))
            print('Report: {}'.format(report_id))
            print('Batch: {} >> {} Completed\nRun Time: {}\n'.format(param['startDate'][0:10],
                                                                     param['endDate'][0:10], run_time))
            return data

    async def generate_reports(self, loop: AbstractEventLoop, params: List[Dict], report_id: str) -> List[pd.DataFrame]:
        async with aiohttp.ClientSession(loop=loop, headers=self.request_headers) as api_session:
            results = await asyncio.gather(*[self.run_report(api_session, param, report_id) for param in params],
                                           return_exceptions=True)
            return results

    def run_report_batches(self, report_id: str) -> NoReturn:
        batch_params = [{'saveAsFile': 'false', 'includeHeaders': 'true', 'startDate': start, 'endDate': end}
                        for start, end in self.date_batches]

        event_loop = asyncio.get_event_loop()
        futures = asyncio.ensure_future(self.generate_reports(event_loop, batch_params, report_id))
        self.report_df_batches = event_loop.run_until_complete(futures)

    def get_full_report(self) -> pd.DataFrame:
        # merge all batch df's
        return pd.concat(self.report_df_batches, ignore_index=True, sort=False)

    @staticmethod
    def format_data(raw: str) -> pd.DataFrame:
        data_string = StringIO(base64.b64decode(raw).decode('ascii'))
        data = pd.read_csv(data_string, sep=',', low_memory=False)
        return data

    @staticmethod
    def get_date_range(start_date: str, end_date: str) -> List[Tuple[str, str]]:
        # Check if date string is correct
        def check_date(date: str) -> bool:
            try:
                parse(date, fuzzy=False)
                return True
            except ValueError:
                return False

        if check_date(start_date) and check_date(end_date):
            # month START dates in ISO formatted strings
            start_times = [time.isoformat() for time in pd.Series(pd.date_range(start=start_date, end=end_date,
                                                                                freq='MS'))]
            # month END dates in ISO formatted strings
            end_times = [time.isoformat() for time in pd.Series(pd.date_range(start=start_date, end=end_date,
                                                                              freq='M'))]

            return list(zip(start_times, end_times))
        else:
            raise ValueError('Date string params should be in format: YYYY-MM-DD')


if __name__ == '__main__':
    start = '2019-01-01'
    end = '2019-05-01'

    reports = {'521': HistoricalReport(start, end),
               '524': HistoricalReport(start, end)
               }

    for key in reports.keys():
        reports[key].run_report_batches(key)

    for key in reports.keys():
        print(key, 'Data Shape: ', reports[key].get_full_report().shape)
