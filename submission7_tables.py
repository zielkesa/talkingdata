

import MySQLdb
import pandas as pd 

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(query):
    """runs a mysql query and returns a dataFrame"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    df = pd.DataFrame.from_records(dbFetched)
    return df

trainRows = 184903890
totalAttributed = 456846

#Look up table for clicks per day from each ip address.
day1 = """CREATE TABLE day 
          SELECT ip, DAY(click_time) AS day, 
          COUNT(ip) AS clicks, SUM(is_attributed) AS attributed 
          FROM train
          GROUP BY ip, day;"""

day2 = """SELECT clicks, COUNT(clicks) AS total_clicks, 
          SUM(attributed) AS attributed 
          FROM day
          GROUP BY clicks;"""

day3 = """DROP TABLE day;"""

db.query(day1)
df = run_query(day2)
pAttributed = []
pClicks = []
for index, row in df.iterrows():
    total = row['day.clicks'] * row['total_clicks']
    pAttributed.append(row['attributed'] / total)
    pClicks.append(total / trainRows)

df['pAttributed'] = pAttributed
df['pClicks'] = pClicks
df.to_csv('processing/submission7/day.csv', index = False)
db.query(day3)

print('got day')

#look up table for clicks per hour from each ip address
hour1 = """CREATE TABLE hour 
           SELECT ip, HOUR(click_time) AS hour, 
           COUNT(ip) AS clicks, SUM(is_attributed) AS attributed 
           FROM train 
           GROUP BY ip, hour;"""

hour2 = """SELECT clicks, COUNT(clicks) AS total_clicks, 
           SUM(attributed) AS attributed 
           FROM hour
           GROUP BY clicks;"""

hour3 = """DROP TABLE hour;"""

db.query(hour1)
df = run_query(hour2)
pAttributed = []
pClicks = []
for index, row in df.iterrows():
    total = row['hour.clicks'] * row['total_clicks']
    pAttributed.append(row['attributed'] / total)
    pClicks.append(total / trainRows)

df['pAttributed'] = pAttributed
df['pClicks'] = pClicks
df.to_csv('processing/submission7/hour.csv', index = False)
db.query(hour3)

print('got hour')

#Look up table for ip addresses
query = """SELECT ip, COUNT(ip) AS total_clicks, 
           SUM(is_attributed) AS attributed
           FROM train
           GROUP BY ip;"""

df = run_query(query)
pAttributed = []
pClicks = []
for index, row in df.iterrows():
    pAttributed.append(row['attributed'] / row['total_clicks'])
    pClicks.append(row['total_clicks'] / trainRows)

df['pAttributed'] = pAttributed
df['pClicks'] = pClicks
df.to_csv('processing/submission7/ip.csv', index = False)

#Look up table for IP addresses used
query = """SELECT ip, COUNT(ip) AS clicks 
         FROM test
         GROUP BY ip;"""

df = run_query(query)
df.to_csv('processing/submission7/dayTest.csv', index = False)
print('got day')

query = """SELECT ip, HOUR(click_time) AS hour, COUNT(ip) AS clicks 
         FROM test
         GROUP BY ip, hour;"""

df = run_query(query)
df.to_csv('processing/submission7/hourTest.csv', index = False)
print('got hour')

