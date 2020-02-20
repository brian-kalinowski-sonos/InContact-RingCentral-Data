import base64
from io import StringIO
from typing import *
import pandas as pd
import requests
from dateutil.parser import parse
from TokenManager import TokenManager


class InContactReport:
    def __init__(self, start_date: str, end_date: str):
        # TODO change this for service account
        self.token = TokenManager('creds.yml')

        # gets 30 day intervals of dates from start till end
        self.date_batches = self.get_date_range(start_date, end_date)

        # holds the df for each batch in the date range
        self.report_df_batches = []

        # Default API headers
        self.request_headers = {'Authorization': 'Bearer {}'.format(self.token()),
                                'Content-Type': 'x-www-form-urlencoded',
                                'Accept': 'application/json'
                                }

    # TODO change these print thing to logging
    def run_report_batches(self, report_id: str) -> NoReturn:
        # InContact data download API endpoint
        url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/{}'.format(report_id)

        for start, end in self.date_batches:
            try:
                print('Running Batch: {} TO {}'.format(start[0:10], end[0:10]))

                # make the API request for the 30day batch of data
                request_params = {'saveAsFile': 'false', 'includeHeaders': 'true', 'startDate': start, 'endDate': end}
                api_response = requests.post(url, params=request_params, headers=self.request_headers)
                api_response.raise_for_status()

                # format exception if bad http response
                if api_response.status_code != 200:
                    raise Exception('API HTTP ERROR: {}\nJSON response: {}'.format(api_response.status_code,
                                                                                   api_response.json()))

                # decode the Encoded API response String
                data_string = StringIO(base64.b64decode(api_response.json()['file']).decode('ascii'))

                # format as a DataFrame
                report_df = pd.read_csv(data_string, sep=',', low_memory=False)

                # append to list of all report batches
                self.report_df_batches.append(report_df)
                print('Finished Batch: {} TO {}\n'.format(start[0:10], end[0:10]))

            except Exception as exp:
                print(exp)

    def get_full_report(self) -> pd.DataFrame:
        # merge all batch df's
        return pd.concat(self.report_df_batches, ignore_index=True, sort=False)

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
