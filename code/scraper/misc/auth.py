from twitter import Twitter, OAuth, TwitterHTTPError

scraperID='DO'


mailCredentials = {
'apiKey' : 'key-1e51c1bdd96464b5f207c148f45eaef3',
'recipients' : ['casyfill@gmail.com'],
'address' : 'mailgun@mg.foobar.com'  
}

def getTwitter():
    OAUTH_TOKEN = '37474406-jEEBDTrNYnMEYJKhz3RZ2wXyDrXOAw0uB0exv0uQu'
    OAUTH_SECRET = '06KN1JuwvPE1N1o0eZNEDKaiyAgGxJUOmAoBxRqL44'
    CONSUMER_KEY = 'inqHmGzoiG61c68hxd5c7g'
    CONSUMER_SECRET = 'YAYyeNsdEUTI4sNteG3HRovXbB50xN3KEo0QWPkoA'

    return Twitter(
        auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, CONSUMER_KEY, CONSUMER_SECRET))


