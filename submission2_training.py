# Random forest
# got some code from here: 
#    http://blog.yhat.com/posts/random-forests-in-python.html
#

import MySQLdb
import numpy as np
import pandas as pd 
import pickle
from sklearn.ensemble import RandomForestClassifier

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(table, startRow, limit):
    """runs a mysql query and returns a data frame"""
    query = """SELECT os, ip, device, \
               channel, app, \
               YEAR(click_time) AS year, \
               MONTH(click_time) AS month, \
               DAY(click_time) AS date, \
               DAYOFWEEK(click_time) AS day, \
               HOUR(click_time) AS hour, \
               MINUTE(click_time) as minute"""

    if 'train' in table:
        query = query + """, is_attributed"""
    else:
        query = query + """, click_id"""

    query = query + """ FROM """ + table + \
            """ LIMIT """ + str(limit) + \
            """ OFFSET """ + str(startRow) + """;"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    queryDF = pd.DataFrame.from_records(dbFetched)
    return queryDF



# Training
#allFits = []
maxRows = 184903890
trainingRows = 2000000
modelNum = 1

for i in range(0, maxRows, trainingRows):
    print(str(i) + '/' + str(maxRows))
    train = run_query('train', i, trainingRows)
    y = train['train.is_attributed']
    train.drop('train.is_attributed', axis = 1, inplace = True)

    rfc = RandomForestClassifier(n_jobs = 6)
    rfc.fit(train, y)
    #allFits.append(rfc)
    modelName = "processing/submission2/rfc" + str(modelNum) + ".sav"
    modelNum = modelNum + 1
    pickle.dump(rfc, open(modelName, 'wb'))



print('Done Training')


#processing, over 1 to 1. round to 5 decimal places
#click_id to int


