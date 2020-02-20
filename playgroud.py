import requests

from TokenManager import TokenManager

# API access token
token = TokenManager('creds.yml')

# ID of the report to run
report_id = '2812'

job_id = '681205'

# Base URL
url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/report-jobs/{}'.format(job_id)

# Request Headers
headers = {'Authorization': 'Bearer {}'.format(token()),
           'Content-Type': 'x-www-form-urlencoded',
           'Accept': 'application/json'
           }

payload = {'fileType': 'CSV',
           'includeHeaders': 'true',
           'appendDate': 'true',
           'deleteAfter': '7',
           'overwrite': 'true',
           'startDate': '2020-01-01T00:00:00.000',
           'endDate': '2020-01-31T00:00:00.000'
           }

response = requests.get(url, headers=headers)
response.raise_for_status()

print(response.status_code)
print(response.json())
