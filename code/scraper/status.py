import subprocess
import mailer
import auth
import scraper

### this script runs scraper if it is not working - it is meant to be runned from crontab

def findProcess( processId ):
    ps= subprocess.Popen("ps -ef | grep " + processId, shell=True, stdout=subprocess.PIPE)
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()
    return ('python scraper.py' in output)

def main():
	if not findProcess('scraper.py'):
		print 'deploying scraper'
		mailer.send_welcoming()
		scraper.main()
	else:
		print 'everything seems ok\n'

# python start_if_died.py

if __name__ == '__main__':
	print 'howdy how'
	main()

