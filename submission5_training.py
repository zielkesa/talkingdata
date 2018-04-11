# logistic regression
# Training the models

import MySQLdb
import numpy as np
import pandas as pd 
import pickle
from sklearn.linear_model import LogisticRegression

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(table, startRow, limit):
    """runs a mysql query and returns a data frame"""
    query = """SELECT os, ip, device, \
               channel, app, \
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
maxRows = 184903890
trainingRows = 2000000
modelNum = 1

for i in range(0, maxRows, trainingRows):
    print(str(i) + '/' + str(maxRows))
    train = run_query('train', i, trainingRows)
    y = train['train.is_attributed']
    train.drop('train.is_attributed', axis = 1, inplace = True)

    classifier = LogisticRegression(n_jobs = 6, solver = 'sag')
    classifier.fit(train, y)
    modelName = "processing/submission5/lr" + str(modelNum) + ".sav"
    modelNum = modelNum + 1
    pickle.dump(classifier, open(modelName, 'wb'))


print('Done Training')


