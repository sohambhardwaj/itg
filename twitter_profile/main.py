import tweepy
import json
from twitter_new_kw import match_trending
import pymysql
from datetime import datetime, timezone, timedelta
import threading
import time
from track import start_tracking
keys = json.load(open('configs/auth.json', 'r'))



def authentication(key) :

    # Authorization to consumer key and consumer secret
    auth = tweepy.OAuthHandler(key["consumer_key"], key["consumer_secret"])

    # Access to user's access key and access secret
    auth.set_access_token(key["access_key"], key["access_secret"])

    # Calling api
    api = tweepy.API(auth)

    return api

connection = pymysql.connect(host='', user = '',password='', db='')
cursor = connection.cursor()

local_connection = pymysql.connect(host='localhost', user='root', password='', db='youtube_db')
local_cursor = local_connection.cursor()

#Database: db , for tracking users. For schema of the table track_users, see test.py
track_connection = pymysql.connect(host='localhost',user='root',password='12345',db='db')
track_cursor = connection.cursor()

api = authentication(keys)

new_kw = []

def work(api) :

    global new_kwu7jj

    new_df = match_trending(new_kw, cursor)
    print(new_df)
    dnow = datetime.now()
    date_now = dnow.strftime('%Y-%m-%d')
    time_now = dnow.strftime('%H:%M:%S')
    ts = dnow.replace(tzinfo=timezone.utc).timestamp()

    if new_df == [] :
        return 
    print(new_df)
    df = list(set(new_df))
    local_cursor.execute('SELECT `name` from twitter_pro_new_kw')
    li = [items[0] for items in cursor.fetchall()]
    df = [[items] for items in df if not items in li]
    df = [items.extend([date_now, time_now, ts]) for items in df]
    df = [tuple(items) for items in df]
    local_cursor.execute('INSERT INTO `twitter_pro_new_kw` (`name`, `date_added`, `time_added`, `timestamp_added`) VALUES {}'.format(str(df)[1:-1]))

    for rows in df :
        tweets = tweepy.Cursor(api.search, q = rows[0]).items(200)
        li = [items._json['user'] for items in list(tweets)]
        for items in li :
            try :
                created_at = datetime.strptime(items['created_at'], "%a %b %d %H:%M:%S %z %Y")
                created_at = created_at + timedelta(hours = 5, minutes = 30)

                entity_desc_url = None
                try :
                    entity_desc_url = items['entities']['description']['urls'][0]['expanded_url']
                except:
                    pass

                entity_url = None
                try :
                    entity_url = items['entities']['url']['urls'][0]['expanded_url']
                except:
                    pass
                
                cursor.execute("INSERT INTO `twitter_inc_udetails`(`user_id`, `created_at`, `description`, `entity.description.url`, `entity.url`, `following`, `location`, " + 
                            "`name`, `screen_name`, `favourites_count`, `followers_count`, `friends_count`, `statuses_count`, `timestamp_added`) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}', '{}', '{}', '{}', '{}')".format(
                                items['id_str'], created_at.strftime('%Y-%m-%d %H:%M:%S'), items['description'], entity_desc_url, entity_url, items['following'], items['location'], items['name'], items['screen_name'], items['favourites_count'],
                                items['following_count'], items['friends_count'], items['statuses_count'], ts
                            ))

                start_tracking(track_cursor, items['id_str'], items['screen_name'], created_at,items['statuses_count'])
                track_connection.commit()
                connection.commit()
            except Exception as e:
                print(e)


def update_tracking(api):
    #Selct all users who have sample_time in the range [datetime.now()-timedelta(minutes=15),datetime.now()+timedelta(minutes=15) ]
    fifteen_mins_early = datetime.now() - timedelta(minutes = 15)
    fifteen_mins_forward = datetime.now() + timedelta(minutes = 15)
    track_cursor.execute("SELECT `user_id` FROM track_users WHERE sample_time<=`{}` && sample_time>='{}'".format(    fifteen_mins_forward.strftime('%Y-%m-%d %H:%M:%S'),fifteen_mins_early.strftime('%Y-%m-%d %H:%M:%S')))
    query=track_cursor.fetchall();
    if(len(query)==0): 
        return
    for user_id in query:
        new_statuses_count=api.get_user(user_id).statuses_count
        track_cursor.execute("UPDATE `track_users` SET `statuses_count`='{}' WHERE `user_id`='{}'".format(
            new_statuses_count,user_id))
        track_connection.commit()


while True :

    threading.Thread(target=work, args=(api, )).start()
    threading.Thread(target=update_tracking, args=(api, )).start()
    time.sleep(15*60)
    