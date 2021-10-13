import tweepy
import json
import pymysql
from datetime import datetime, timezone, timedelta
import threading
import time
import pandas as pd
woeid=json.load(open('place_id.json','r'))
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

trends = api.trends_place(id = woeid["India"])
for trend in trends[0]['trends']:
    print(trend['name'], trend['tweet_volume'])