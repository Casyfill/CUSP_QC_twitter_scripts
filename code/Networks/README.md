README
======


this c=package consists of:

#### Data

1. sintetic.csv - csv file of "raw" tweet data with 6 fieids, including index, user_id, tweet_id, zipcode, and unix timestamp,UTC time. IT represents all the current data collected from multiple sources for 2016_02_18, without any robot cleaning and (obviously) containing only geolocated tweets.

2. od_matrix.csv - a list of zipcode-zipcode pairs, and a value of tweets for each zipcode in pair, that were made bu users, who tweeted at leas once in both zipcodes, e.g. - probability weights.

Attention: for the code simplicity, there is a pair zip1, zip1 for each zipcode - for those, obviously, numbers are the same, and represent all the tweets that were done in this zipcode by all the users.


#### Code

- do_syntetic.ipynb
- do_syntetic.py

script and notebook on generation synetic data from database

- od_matrix.ipynb
- od_matrix.py

cript and notebook on generation od_matrix