# scheduling.py

# get_tweets every 10 minutes and writes to SQL (assuming database and table created as per write_to_sql)
import sys, os
from datetime import datetime
import time
import re, twitter, codecs, urllib3, traceback
from apscheduler.schedulers.background import BackgroundScheduler
from write_to_sql import TweetActions, DatabaseProperties, DatabaseActions

import logging
logging.basicConfig()

# suppress warnings here; won't need to handle
import warnings
warnings.filterwarnings('ignore')

global last_maxes
global news_accounts

api = twitter.Api(consumer_key='hh3S0ncDBpdq2VyDgCllCGnv5',
                      consumer_secret='zUkc6kM5dgPfWNPt1bCtYK3HpPHqUt2AyPpdoUuZn48zLFpjQ5',
                      access_token_key='3319485942-ZhjimToEWOWRoeIyQI7akaR255G0viUsbZqSMYI',
                      access_token_secret='IUexuQGasZeGlr0JVj82BlGppi2ZSR9tooeAzYQgmHGva')


# MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD HH:MM:SS'
def date_convert(created_at):
	created_at = re.sub(r'\+0000 ','',created_at)
	strip = time.strptime(created_at, '%a %b %d %H:%M:%S %Y')
	for_sql = time.strftime('%Y-%m-%d %H:%M:%S',strip)
	return for_sql

def get_tweets():

	global last_maxes
	t_dict={}
	
	with codecs.open('tweet_dump.txt','a+','utf-8') as w:
		with open('account_info.txt','w') as acc:
			for n,agency in enumerate(news_accounts): 

				# first update attributes of news account (screen_name, followers, friends(following))
				# user = api.GetUser(screen_name=agency)
				# acc.write(agency+'\t')
				# acc.write(str(user.followers_count)+'\t')
				# acc.write(str(user.friends_count)+'\n')

				# now get new tweets from each user
				tweets = api.GetUserTimeline(screen_name=agency,include_rts=False,since_id=last_maxes[n])
				
				# set next since_id (we only want tweets newer than the latest one we just grabbed)
				if tweets:
					last_maxes[n] = tweets[0].id

				twt_dicts = []
				for t in tweets:
					twt = {}
					
					# format date for SQL
					sql_date = date_convert(t.created_at)
					# remove \t and \n for storage
					#cleaned_t = preprocess(t.text)
					
					# make list of dicts for write_to_sql
					twt['tweet_id'] = str(t.id)
					twt['user_name'] = agency
					twt['tweet'] = t.text
					twt['retweets'] = str(t.retweet_count)
					twt['faves'] = str(t.favorite_count)
					twt['created_at'] = sql_date
					twt['cluster_num'] = '0'
					twt['pushed'] = '0'
					
					twt_dicts.append(twt)
				try:
					twt = TweetActions(DatabaseProperties(),twt_dicts)
					twt.insert_tweets()
				except Exception, err:
					print traceback.format_exc()
					print sys.exc_info()[0]

	return
	
if __name__ == '__main__':

	# get list of sites
	with open ('news_list.txt','r') as f:
		urls = f.readlines()
	news_accounts = [re.search('(?<=com/).+', u).group(0) for u in urls]

	# list of maxids for each news_account, n, to be updated after each call of get_tweets
	# start with small value to get default count of new tweets (will vary since API counts retweets even though not retrieved)
	last_maxes = [10000]*len(news_accounts)

	# task scheduling
	scheduler = BackgroundScheduler()
	scheduler.add_job(get_tweets, 'interval', minutes=10)
	scheduler.start()
	print('Press Ctrl+Break to exit')

	try:
		while True:
			time.sleep(2)
	except (KeyboardInterrupt, SystemExit):
		scheduler.shutdown()
