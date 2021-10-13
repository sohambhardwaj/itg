import tweepy
import json
import pymysql
from datetime import datetime, timezone, timedelta
import threading
import time

keys = json.load(open('configs/auth.json', 'r'))

def authentication(key) :

    # Authorization to consumer key and consumer secret
    auth = tweepy.OAuthHandler(key["consumer_key"], key["consumer_secret"])

    # Access to user's access key and access secret
    auth.set_access_token(key["access_key"], key["access_secret"])

    # Calling api
    api = tweepy.API(auth)

    return api

api = authentication(keys)

def start_tracking(cursor, items['id_str'], items['screen_name'], created_at,items['statuses_count']):
	#inserting the user for the first time
	create_time=created_at.strftime('%Y-%m-%d %H:%M:%S')
	start_sample=create_time
	try:
		cursor.execute("INSERT INTO `track_users`(`user_id`, `screen_name`, `discover_time`, `time_delta`,`sample_time`,`stauses_count`) VALUES ('{}','{}','{}','{}','{}','{}')".format(items['id_str'], items['screen_name'],start_sample,0,create_time,items['statuses_count']))

		#making schedule of the above user
		for day in range(1,8):
			create_time=(created_at+timedelta(days=day)).strftime('%Y-%m-%d %H:%M:%S')
			cursor.execute("INSERT INTO `track_users`(`user_id`, `screen_name`, `discover_time`, `time_delta`,`sample_time`,`stauses_count`) VALUES ('{}','{}','{}','{}','{}','{}')".format(items['id_str'], items['screen_name'],start_sample,day,create_time,items['statuses_count']))
	except:
		print("user already on tracking")		
	
		

