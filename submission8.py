

#Import libraries:
import MySQLdb
import pandas as pd
import pickle
import re
from sklearn.ensemble import GradientBoostingClassifier  

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(startRow, subsetRows):
    """runs a mysql query and returns a data frame"""
    query = """SELECT click_id, os, ip, device, \
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

#Training the model
train = pd.read_csv('processing/sample/subTrain.csv')
print("read in train")
target = 'train.is_attributed'
predictors = [x for x in train.columns if x not in [target]]

gbmA = GradientBoostingClassifier(learning_rate=0.1, 
                                 n_estimators=500, max_depth=10, 
                                 min_samples_split=20000, 
                                 min_samples_leaf=2600, 
                                 subsample=0.85, random_state=10, 
                                 max_features=5,
                                 verbose = 1)
gbmB = GradientBoostingClassifier(learning_rate=0.01, 
                                 n_estimators=5000, max_depth=10, 
                                 min_samples_split=20000, 
                                 min_samples_leaf=2600, 
                                 subsample=0.85, random_state=10, 
                                 max_features=5,
                                 verbose = 1)
#gbmA.fit(train[predictors], train[target])
#pickle.dump(gbmA, open('processing/submission8/gbmA.sav', 'wb'))
#print('Model A Trained')
#gbmB.fit(train[predictors], train[target])
#pickle.dump(gbmB, open('processing/submission8/gbmB.sav', 'wb'))
#print('Model B Trained')

gbmA = pickle.load(open('processing/submission8/gbmA.sav', 'rb'))
gbmB = pickle.load(open('processing/submission8/gbmB.sav', 'rb'))
print('read in models')

#Predicting
maxRows = 18790469
predRows = 1000000

predictions = pd.DataFrame(columns = ['click_id', 'is_attributed'])
predictions.to_csv('submissions/submission8a.csv', index = False)
predictions.to_csv('submissions/submission8b.csv', index = False)

for i in range(0, maxRows, predRows):
    print(str(i) + '/' + str(maxRows))
    test = run_query(i, predRows)
    #clickID = pd.Series(list(test['test.click_id']))
    predictionsDF = pd.DataFrame(pd.Series(list(test['test.click_id'])))
    colNames = test.columns
    colNames = [re.sub("test", "train", x) for x in colNames]
    test.columns = colNames

    testPredictionsA = gbmA.predict(test[predictors])
    testPredictionsB = gbmB.predict(test[predictors])
    predictionsDF['is_attributed'] = pd.Series(testPredictionsA)

    #predictionsA.columns = ['click_id', 'is_attributed']
    with open('submissions/submission8a.csv', 'a') as f:
        predictionsDF.to_csv(f, header = False, index = False)
    predictionsDF['is_attributed'] = pd.Series(testPredictionsB)
    with open('submissions/submission8b.csv', 'a') as f:
        predictionsDF.to_csv(f, header = False, index = False)

