### write_to_sql.py
### Called by scheduling.py, using MySQLdb to write downloaded tweets to database
'''
Need to run the following in root so that db is created; also create table NewsTweets

create database news_tweets;
create user 'defaultuser'@'localhost' identified by '';
grant all privileges on *.* to 'defaultuser'@'localhost'

create table if not exists NewsTweets (

tweet_id long,
user_name varchar(50),
tweet text character set utf8 not null,
retweets int,
faves int,
created_at datetime, # MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD HH:MM:SS'
cluster_num int,
pushed tinyint(1) #boolean

);
'''
from __future__ import division
import MySQLdb
import sys
from os import listdir
from os.path import isfile, join
import traceback


class DatabaseProperties():
    def __init__(self):
        # Should have this initialized
        self.hostname = "localhost"
        self.user = "defaultuser"
        self.password = ""
        self.dbName = "news_tweets"
        
        self.tableName = "NewsTweets"
        
        self.dbToVarMap = {'Tweet ID': 'tweet_id',
                           'News Account':'user_name',
                           'Tweet':'tweet',
                           'Retweets':'retweets',
                           'Favourites':'faves',
                           'Created Time':'created_at',
                           'Cluster Number':'cluster_num',
                           'Pushed':'pushed',
                           }

class DatabaseActions(object):
    # base class for add, update, and remove
    def __init__(self, dbProperties):
        self.dbProperties = dbProperties
        self.mysqlconnect()
    
    def mysqlconnect(self):
        try:
            self.conn = MySQLdb.connect(self.dbProperties.hostname, 
                                   self.dbProperties.user, 
                                   str(self.dbProperties.password),
                                   self.dbProperties.dbName)
            self.cursor = self.conn.cursor()
			
			################### check
            self.conn.set_character_set('utf8')
            self.cursor.execute('SET NAMES utf8;')
            self.cursor.execute('SET CHARACTER SET utf8;')
            self.cursor.execute('SET character_set_connection=utf8;')
			
        except MySQLdb.Error, e:
            print e.args
            print 'Error: %d %s' %(e.args[0], e.args[1])
            sys.exit(1)
    
    def insert(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except MySQLdb.Error, e:
            print e.args
            print 'Error: %d %s' %(e.args[0], e.args[1])
            
    def processErrors(self, elem):
        if elem in self.dbProperties.dbErrorList:
            return None
        else:
            return elem
    
    def __del__(self):
        # Close cursor
        self.cursor.close()
        self.conn.close()
        print 'DB connection closed!'
    
class TweetActions(DatabaseActions):

    TABLE_NAME = "NewsTweets"
    INSERT_SQL_TEMPLATE = "insert into " + TABLE_NAME + " (%s) values (%s)"
    #INSERT_DATE_TEMPLATE = "insert into " + TABLE_NAME + "('date') value (%s))" 
    TABLE_COLUMNS = ['tweet_id','user_name','tweet','retweets','faves','created_at','cluster_num','pushed']
	
    def __init__(self, dbProperties, twtList):
        super(TweetActions, self).__init__(dbProperties)
        self.twtList = twtList

    def insert_tweets(self):
		column_string = ",".join(self.TABLE_COLUMNS)
		for twt in self.twtList:    #rev = one dict in revList
            #print type(rev)
            # iterate through the dictionary, making a string of column keys and values
			value_string_arr = []
			for column in self.TABLE_COLUMNS:
                #print column
				try:
					#twtstr = str(twt[column]).encode("utf-8")
					twtstr = str(twt[column]).replace("'", "\\'")
				except:
					print 'twtstr exception! abandon to avoid null entry'
					print twtstr
					twtstr = ''
                #print revstr
				value_string_arr.append("'" + twtstr + "'")
			value_string = ",".join(value_string_arr)
			insertCommand = self.INSERT_SQL_TEMPLATE %(column_string, value_string)
			self.insert(insertCommand)



if __name__ == "__main__":
	''' below for testing
	test_dict = {'tweet_id':'635632659474063360','user_name': 'nytimes','tweet':'A year after a girl accidentally killed someone, it\'s business as usual at this gun range http://t.co/qtslFCfMwr http://t.co/7XrrDreS3k', 'retweets':'54','faves':'60','created_at':'2015-01-08 02:00:10','cluster_num':'4','pushed':'0'}

	try:
		twt = TweetActions(DatabaseProperties(),[test_dict])#([test_dict])
		twt.insert_tweets()
	except Exception, err:
		print traceback.format_exc()
		#or
		print sys.exc_info()[0]
	'''
