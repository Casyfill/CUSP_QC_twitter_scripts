from twitter import Twitter, OAuth, TwitterHTTPError


mailCredentials = {
'apiKey' : 'APIKEY',
'recipients' : ['casyfill@gmail.com'],
'address' : 'mailgun@mg.foobar.com'  
}

def getTwitter():
    OAUTH_TOKEN = 'OAUTH_TOKEN'
    OAUTH_SECRET = 'OAUTH_SECRET'
    CONSUMER_KEY = 'CONSUMER_KEY'
    CONSUMER_SECRET = 'CONSUMER_SECRET'

    return Twitter(
        auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, CONSUMER_KEY, CONSUMER_SECRET))


