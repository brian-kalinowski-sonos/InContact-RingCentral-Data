import base64
from io import StringIO

import pandas as pd
import requests

from InContactData.TokenManager import TokenManager

# API access token
token = TokenManager('creds.yml')

# ID of the report to run
report_id = '544'

# file name for report results
file_name = 'agent_state_log.csv'

# Base URL
url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/{}'.format(report_id)

# Request Headers
headers = {'Authorization': 'Bearer {}'.format(token()),
           'Content-Type': 'x-www-form-urlencoded',
           'Accept': 'application/json'
           }

params = {'saveAsFile': 'false',
          'includeHeaders': 'true',
          'startDate': '2019-01-01T00:00:00',
          'endDate': '2019-01-31T00:00:00'
          }

try:
    response = requests.post(url, params=params, headers=headers)
    response.raise_for_status()

    if response.status_code != 200:
        print('HTTP ERROR')
        print(response.json())

    # base64 encoded string
    response_raw = response.json()['file']

    # String IO format
    data_string = StringIO(base64.b64decode(response_raw).decode('ascii'))

    # DataFrame
    data_frame = pd.read_csv(data_string, sep=',')
    print('Creating file...\n')
    print('Data Shape: {}'.format(data_frame.shape))
    print(data_frame.head())

except Exception as exp:
    print(exp)
