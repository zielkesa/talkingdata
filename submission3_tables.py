#Creating the lookup tables for submission 3
#This is an extension of submission 1, adding the ip column

import MySQLdb
import pandas as pd 

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

trainRows = 184903890 #Rows in test file
trainTable = 'train'

attributed = 456846

def run_query(query):
    """runs a mysql query and returns a the result"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    return dbFetched

def write_csv(df, fileName):
    df.to_csv('processing/submission3/' + fileName + '.csv', index = False)

# app (check to see how many apps there are)
query = """SELECT ip, COUNT(ip) AS ip_attributed 
           FROM train 
           WHERE is_attributed = 1
           GROUP BY ip;"""
pIPAt = pd.DataFrame.from_records(run_query(query))
pIPAt.columns = ['ip_attributed', 'ip']
pIPAt['pIP'] = pIPAt['ip_attributed'] / attributed
write_csv(pIPAt, 'IPAttributed')


query = """SELECT ip, COUNT(ip) AS ip_attributed 
           FROM train 
           GROUP BY ip;"""
pIP = pd.DataFrame.from_records(run_query(query))
pIP.columns = ['ip_count', 'ip']
pIP['pIP'] = pIP['ip_count'] / trainRows
write_csv(pIP, 'pIP')

