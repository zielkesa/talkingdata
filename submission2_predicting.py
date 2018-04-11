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

totalFits = 93
allFits = []
for i in range(totalFits):
    allFits.append(pickle.load(open('processing/submission2/rfc' + 
                                    str(i + 1) + '.sav', 'rb')))

# Predicting

maxRows = 18790469
trainingRows = 500000

predHeaders = pd.DataFrame(columns = ['click_id'] + 
                                     list(range(len(allFits))))
predHeaders.to_csv('processing/submission2/allPreds.csv', index = False)

for i in range(0, maxRows, trainingRows):
    print(str(i) + '/' + str(maxRows))
    #Each list is the predictions for one fit
    allPreds = [list() for i in allFits] 
    test = run_query('test', i, trainingRows)
    clickID = list(test['test.click_id'])
    test.drop('test.click_id', axis = 1, inplace = True)
    for i in range(len(allFits)):
        allPreds[i].extend(allFits[i].predict(test))
    allPredsDF = pd.DataFrame(pd.Series(clickID), columns = ['click_id'])
    for i in range(len(allPreds)):
        allPredsDF[str(i)] = pd.Series(allPreds[i])
    with open('processing/submission2/allPreds.csv', 'a') as f:
        allPredsDF.to_csv(f, header = False, index = False)




