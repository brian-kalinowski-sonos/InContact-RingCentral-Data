import base64
import json
import time
from typing import *

import requests
import yaml


# Class to hold the current token, etc. for in-contact
class Token:
    def __init__(self, username: str, password: str, auth_code: str):
        request_payload = json.dumps({'grant_type': 'password', 'username': username,
                                      'password': password, 'scope': 'ReportingApi'})

        request_header = {'Content-Type': 'application/json', 'Authorization': auth_code}

        try:
            response = requests.post('https://api.incontact.com/InContactAuthorizationServer/Token',
                                     data=request_payload, headers=request_header)

            response.raise_for_status()
            self.token_attrs = response.json()
        except requests.exceptions.HTTPError as exp:
            print(exp)
            self.token_attrs = None

    def update_token(self, new_attrs: dict) -> NoReturn:
        if new_attrs is not None:
            self.token_attrs = new_attrs
        else:
            print('Error Token Not updated')

    def __call__(self, *args, **kwargs) -> dict:
        return self.token_attrs


# Class that manages the refresh of the token so it will not expire
class TokenManager:
    def __init__(self, credentials_path: str):
        self.credentials = self.get_credentials(credentials_path)
        self.token = Token(self.credentials['user'], self.credentials['pwd'], self.credentials['auth'])

        # set the expiration to 1 minute before the expire time
        self.token_expiration = time.time() + (self.token()['expires_in'] - 60.0)

    def __call__(self, *args, **kwargs) -> str:
        # refresh if the current time is past the expire time
        if time.time() >= self.token_expiration:
            self.refresh_token()
            self.token_expiration = time.time() + (self.token()['expires_in'] - 60.0)
        return self.token()['access_token']

    def refresh_token(self) -> NoReturn:
        refresh_payload = json.dumps({'grant_type': 'refresh_token', 'refresh_token': self.token()['refresh_token']})
        refresh_header = {'Content-Type': 'application/json', 'Authorization': self.credentials['auth']}

        try:
            response = requests.post('https://api-c30.incontact.com/inContactAuthorizationServer/Token',
                                     data=refresh_payload, headers=refresh_header)

            response.raise_for_status()
            data = response.json()
            self.token.update_token(data)
        except requests.exceptions.HTTPError as exp:
            print(exp)

    @staticmethod
    def get_credentials(path) -> dict:
        with open(path) as cred_file:
            contents = yaml.safe_load(cred_file)
        info = contents['in_contact']
        info['auth'] = 'basic {}'.format(base64.b64encode(info['auth'].encode('utf-8')).decode('utf-8'))
        return info


# example with forced refresh

# if __name__ == '__main__':
#     token = TokenManager('creds.yml')
#     for i in range(4):
#         token.refresh_token()
#         print('Refresh Token: ', token.token()['refresh_token'])
#         print('Current Token: ', token()[0:10], '......etc', '\n')
