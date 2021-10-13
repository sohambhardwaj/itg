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

# trends = api.trends_place(id = woeid["India"])
# print(trends[0]['trends'][0])



def add_trending(df,count):
    current_time=(datetime.now()+timedelta(hours=5, minutes=30)).strftime("%H:%M")
    current_date=str(datetime.now().date())
    for id,loc in enumerate(woeid,1):
        trends=api.trends_place(id=woeid[loc])
        for rank,trend in enumerate(trends[0]['trends'],1):
            print(trend['name'])
            t_v=trend['tweet_volume']
            if(trend['tweet_volume']==None):
                t_v=0 
            df=df.append({'id':count,'name':trend['name'],'Rank':rank,'Tweet_Volume':t_v,'Region':loc,'Date':current_date,'Time':current_time,'Time1':(datetime.now()+timedelta(hours=5, minutes=30)).strftime("%H:%M")},ignore_index=True)
            count+=1

df=pd.DataFrame(columns=['id', 'name', 'Rank','Tweet_Volume','Region','Date','Time','Time1'])
countFiveMin=0
count=1
while(True):
    if(countFiveMin==288) :
        df.to_excel("files/"+str(datetime.now().date())+".xlsx",index=False)
        wdf=df.groupby('name')['Rank'].sum().reset_index()
        wdf.to_excel("files/weights_" + str(datetime.now().date()) + ".xlsx", index=False)
        df=pd.DataFrame(columns=['id', 'name', 'Rank','Tweet_Volume','Region','Date','Time','Time1'])
        countFiveMin=0
        
    add_trending(df,count)
    time.sleep(5*60)
    countFiveMin+=1
    count+=1