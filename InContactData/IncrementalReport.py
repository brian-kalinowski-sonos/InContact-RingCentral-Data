import base64
from io import StringIO

import pandas as pd
import requests

from TokenManager import TokenManager


class IncrementalReport:
    def __init__(self, report_id: str, start_date: str):
        # Todo rework token stuff
        self.token = TokenManager('/Users/brian.kalinowski/IdeaProjects/RingCentralData/creds.yml')
        self.report_id = report_id

        self.start_date = pd.to_datetime(start_date)
        self.end_date = (self.start_date + pd.DateOffset(days=1))
        self.data = None

    def run_report(self):
        url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/' + self.report_id

        request_header = {'Authorization': 'Bearer {}'.format(self.token()),
                          'Content-Type': 'x-www-form-urlencoded',
                          'Accept': 'application/json'
                          }

        request_params = {'saveAsFile': 'false',
                          'includeHeaders': 'true',
                          'startDate': self.start_date.isoformat(),
                          'endDate': self.end_date.isoformat()
                          }
        try:
            api_response = requests.post(url=url, params=request_params, headers=request_header)
            api_response.raise_for_status()
            data_string = api_response.json()['file']
            self.data = self.format_data(data_string)

        except Exception as exp:
            self.data = pd.DataFrame()
            print(exp)

    @staticmethod
    def format_data(raw: str) -> pd.DataFrame:
        data_string = StringIO(base64.b64decode(raw).decode('ascii'))
        data = pd.read_csv(data_string, sep=',', low_memory=False)
        return data
