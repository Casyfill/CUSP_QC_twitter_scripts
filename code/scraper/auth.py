from twitter import Twitter, OAuth, TwitterHTTPError


mailCredentials = {
'apiKey' : 'APIKEY',
'recipients' : ['casyfill@gmail.com'],
'address' : 'mailgun@mg.foobar.com'  
}

def getTwitter():
    OAUTH_TOKEN = '4898892569-WvHwkfkNv9IU5SkEpJvum0zuTF8Okk2yxnOX92S'
    OAUTH_SECRET = 'PDbp3b8IP94IimG0dcpokE4X1xP4xo0vBDq14vqAglmXG'
    CONSUMER_KEY = 'rQOjxtOoA3BazXUkqOUsaPij0'
    CONSUMER_SECRET = 'W4tyXW75WwBJhbuSDD8DRUrTk7qn8dsJjEsdm2sVLFRfQISjsO'

    return Twitter(
        auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, CONSUMER_KEY, CONSUMER_SECRET))


