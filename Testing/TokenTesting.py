from TokenManager import TokenManager

token = TokenManager('/Users/brian.kalinowski/IdeaProjects/RingCentralData/creds.yml')

for idx in range(5):
    print('Token: ', idx)
    print(token()[0:20], '...')
    token.refresh_token()
