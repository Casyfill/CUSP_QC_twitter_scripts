
## Script commands

#### Copy database
mv twitter.db twitterBackup.db -b


#### Crone tab
#Twitter Scraping 
0,30 * * * * echo ‘checking twitter scraper’ >> twitter/scraping.log
0,30 * * * * python twitter/twitter_geo_scraper/status.py >> twitter/scraping.log
