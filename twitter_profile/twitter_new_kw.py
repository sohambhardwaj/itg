import pymysql
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
from tqdm import tqdm
import os

lookup = pd.read_excel('lookup_file.xlsx')

print(lookup)

def match_trending(notifs, cursor) :

    five_mins_early = datetime.now() - timedelta(minutes = 5)
    string = "SELECT `name`, `Rank`, `Tweet_Volume`, `Region` FROM `twitter_trending` WHERE `Date` = '{}' AND `Time` > '{}'". \
        format(five_mins_early.strftime('%Y-%m-%d'), five_mins_early.strftime('%H:%M:%S'))
    print(string)
    cursor.execute(string)
    latest_trending = cursor.fetchall()

    for idx, row in tqdm(lookup.iterrows()) :
        for items in latest_trending :
            if row['Slug'].lower() in items[0].lower() or items[0].lower() in row['Slug'].lower():
                notifs.append(items[0])

    return notifs

# while True :
#     threading.Thread(target = match_trending).start()
#     time.sleep(5 * 60)
#     send_mail(FILENAME=name, RECIPIENT='aditya.rustagi@aajtak.com')
#     os.remove(name)
