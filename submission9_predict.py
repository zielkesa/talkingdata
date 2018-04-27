

import gc
import itertools
import lightgbm as lgb
import MySQLdb
import pandas as pd
import re

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(startRow, subsetRows):
    """runs a mysql query and returns a data frame"""
    query = """SELECT click_id, os, device, \
               channel, app, \
               HOUR(click_time) AS hour, \
               MINUTE(click_time) as minute \
               FROM test \
               LIMIT """ + str(subsetRows) + \
               """ OFFSET """ + str(startRow) + """;"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    queryDF = pd.DataFrame.from_records(dbFetched)
    return queryDF

#Reading models
lgb1M = lgb.Booster(model_file = 'processing/submission9/model1M.txt')
lgb3M = lgb.Booster(model_file = 'processing/submission9/model3M.txt')
lgb5M = lgb.Booster(model_file = 'processing/submission9/model5M.txt')

#Predicting
maxRows = 18790469
predRows = 500000

predictions = pd.DataFrame(columns = ['click_id', 'is_attributed'])
predictions.to_csv('submissions/submission9-1M.csv', index = False)
predictions.to_csv('submissions/submission9-3M.csv', index = False)
predictions.to_csv('submissions/submission9-5M.csv', index = False)

for i in range(0, maxRows, predRows):
    print(str(i) + '/' + str(maxRows))
    test = run_query(i, predRows)
    test.columns = [re.sub("test.", "", x) for x in test.columns]

    #creating features
    print('Creating features...')
    predictors = ['app', 'device', 'os', 'channel', 'hour', 'minute']
    combinations = list(itertools.combinations(predictors[:-1], 4)) + \
                   list(itertools.combinations(predictors[:-1], 3)) + \
                   list(itertools.combinations(predictors[:-1], 2))
    combinations.append(predictors[:-1])

    for j in combinations:
        colName = '_'.join(j)
        predictors = predictors + [colName]
        temp = test[list(j) + 
            ['click_id']].groupby(j)['click_id'].count().reset_index().rename(columns =
            {'click_id': colName})
        test = test.merge(temp, on = j, how = 'left')

    predictionsDF = pd.DataFrame(pd.Series(list(test['click_id'])))
    test = test.drop(['click_id'], axis = 1)

    #Predicting
    print('Predicting...')
    testPredictions1M = lgb1M.predict(test)
    testPredictions3M = lgb3M.predict(test)
    testPredictions5M = lgb5M.predict(test)

    #Writing predictions
    print('Writing predictions...')
    predictionsDF['is_attributed'] = pd.Series(testPredictions1M)
    with open('submissions/submission9-1M.csv', 'a') as f:
        predictionsDF.to_csv(f, header = False, index = False)

    predictionsDF['is_attributed'] = pd.Series(testPredictions3M)
    with open('submissions/submission9-3M.csv', 'a') as f:
        predictionsDF.to_csv(f, header = False, index = False)

    predictionsDF['is_attributed'] = pd.Series(testPredictions5M)
    with open('submissions/submission9-5M.csv', 'a') as f:
        predictionsDF.to_csv(f, header = False, index = False)

    del test, testPredictions1M, testPredictions3M, testPredictions5M
    gc.collect()

