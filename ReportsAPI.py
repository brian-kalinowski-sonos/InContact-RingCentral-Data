import requests
import base64
import csv
from TokenManager import TokenManager

# API access token
token = TokenManager('creds.yml')

# ID of the report to run
report_id = '524'

# file name for report results
file_name = 'call_detail.csv'

# Base URL
url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/datadownload/{}'.format(report_id)

# Request Headers
headers = {'Authorization': 'Bearer {}'.format(token()),
           'Content-Type': 'x-www-form-urlencoded',
           'Accept': 'application/json'
           }


# Request/Report Parameters
# TODO figure out where the saveAsFile parameter actually saves the file
# This is the error recived when trying to GET request the returned URL for the file:
# {'error': 'access_denied', 'error_description': 'The Authorization given does not have access to this resource.'}

params = {'saveAsFile': 'false',
          'includeHeaders': 'true',
          'startDate': '2020-01-01T00:00:00.000',
          'endDate': '2020-01-31T00:00:00.000'
          }

try:
    response = requests.post(url, params=params, headers=headers)
    response.raise_for_status()
    response_raw = response.json()['file']  # base64 encoded string
    print('Creating file...\n')

    data = [line.split(',') for line in base64.b64decode(response_raw).decode('ascii').split('\n')]
    with open(file_name, 'w') as test_file:
        writer = csv.writer(test_file)
        writer.writerows(data)
    print('File Created, {} rows writen'.format(len(data)))

except Exception as exp:
    print(exp)
