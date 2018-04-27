
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

