import requests

from TokenManager import TokenManager

# token = TokenManager('creds.yml')
#
# url = 'https://api-c30.incontact.com/inContactAPI/services/v17.0/'
#
# headers = {'Authorization': 'Bearer {}'.format(token()),
#            'Content-Type': 'x-www-form-urlencoded',
#            'Accept': 'application/json'
#            }
#
# params = {'startDate': '2020-01-01T00:00:00.000', 'endDate': '2020-02-01T00:00:00.000'}
#
# response = requests.get(url + 'skills/sla-summary', headers=headers, params=params)
#
# print(response.json())


import pandas as pd
data = pd.read_csv('call_detail.csv')
print(data.shape)
print(data.head())
