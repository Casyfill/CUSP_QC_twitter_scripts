import subprocess
import mailer
import auth
import scraper
from datetime import datetime

### this script runs scraper if it is not working - it is meant to be runned from crontab

def findProcess( processId ):
	'''mambo jumbo checking if process exists'''
    ps= subprocess.Popen("ps -ef | grep " + processId, shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()
    return ('python scraper.py' in output)

def main():
	'''action - checking if process exists, 
	starting if not, and logging the result anyway'''

	if not findProcess('scraper.py'):
		print '%s | deploying new scraper\n' % datetime.now().strftime('%Y-%m-%d %H:%M')
		mailer.send_welcoming()
		scraper.main()
	else:
		print '%s | scraper working\n' % datetime.now().strftime('%Y-%m-%d %H:%M')

# python start_if_died.py

if __name__ == '__main__':
	main()

