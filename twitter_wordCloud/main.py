import pymysql
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from os import path
from PIL import Image
from wordcloud import WordCloud
connection = pymysql.connect(host='spar-db-instance.cm5prurrbyte.ap-south-1.rds.amazonaws.com', user = 'admin',
                                    password='n204K0FxjCMtqyiDItRh', db='youtube_db')

cursor=connection.cursor()
def work():
    if((datetime.now() + timedelta(hours=5, minutes=30)).time().strftime("%H:%M")=="00:00") :
        prevDayDate=str( (datetime.now()-timedelta(days=1)).date() )
        sql = "SELECT * FROM twitter_trending where Date='{}' AND Region NOT IN ('United States','United Kingdom','Germany') ".format(prevDayDate)
        df = pd.read_sql(sql, cursor)
        df['weight']=0
        for i in range(0,len(df)):
            df['weight'][i]=51-df['Rank'][i]
        df.drop(['ID','Rank','Region','Date','Time','Tweet_Volume'],axis=1, inplace=True)
        df=df.groupby('name')['weight'].sum().reset_index()
        data = dict(zip(df['name'].tolist(), df['weight'].tolist()))
        wc = WordCloud(width=800, height=400, max_words=200).generate_from_frequencies(data)
        # plt.figure(figsize=(10, 10))
        # plt.imshow(wc, interpolation='bilinear')
        # plt.axis('off')
        wc.to_file("img/"+ prevDayDate +".png")
    else:
        return


while(1):
    work()