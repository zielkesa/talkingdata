# implementing a lightGBM model based on this kernal
# https://www.kaggle.com/bk0000/non-blending-lightgbm-model-lb-0-977
#https://github.com/Microsoft/LightGBM

import gc
import itertools
import lightgbm as lgb
import pandas as pd
import re
from sklearn.model_selection import train_test_split
from sklearn.metrics import auc




#Important variables
trainFile = 'processing/sample/subTrain3M.csv'
testRows = 18790469
testChunk = 1000000
predictors = ['app', 'device', 'os', 'channel', 'hour', 'minute']
categorical = predictors[:-2]
target = 'is_attributed'

#Controls
debug = False


if debug:
    print('*** Running in debug mode ***')
    trainFile = 'processing/sample/subTrain.csv'
    testRows = 1000
    testChunk = 100

train = pd.read_csv(trainFile)
train.columns = [re.sub("train.", "", x) for x in train.columns]


#Creating new features
combinations = list(itertools.combinations(predictors[:-1], 4)) + \
               list(itertools.combinations(predictors[:-1], 3)) + \
               list(itertools.combinations(predictors[:-1], 2))
combinations.append(predictors[:-1])

for i in combinations:
    print(i)
    colName = '_'.join(i)
    predictors = predictors + [colName]
    temp = train[list(i) + [target]].groupby(i)[target].count().reset_index().rename(columns = {target: colName})
    train = train.merge(temp, on = i, how = 'left')



#lgbTrain = lgb.Dataset(train, trainY, categorical_feature=categorical)

print(predictors)
print(train.head())
#train.to_csv('processing/submission9/extrafeat.csv', index = False)

#Define model
lgbParams = {'boosting_type': 'gbdt',
          'objective': 'binary',
          'num_threads': 1,
          'metric': 'auc',
          'verbose': 1,
          'num_boost_round': 175, #Tune
          'learning_rate': 0.1,
          'num_leaves': 160, #Tune
          'max_depth': -1,
          'min_data_in_leaf': 450, #Tune
          'feature_fraction': 0.35, #Tune
          'bagging_fraction': 1.0, #Tune
          'bagging_freq': 1, #Tune
          'bin_construct_sample_cnt': 20000} #Tune



#Tuning

print('Tuning num_boost_round')

def run_cv_search(testParam, testValues):
    results = pd.DataFrame(testValues, columns = ['testValues'])

    for i in range(3):
        iterationResults = []
        X_train, X_test, y_train, y_test = train_test_split(
            train, train[target], test_size=0.34, random_state=i)
        lgbTrain = lgb.Dataset(X_train, y_train, 
            categorical_feature=categorical, free_raw_data = False)

        for j in testValues:
            lgbParams[testParam] = j
            print('iteration: ' + str(i) + ' value: ' + str(lgbParams[testParam]))
            lgbModel = lgb.train(lgbParams, lgbTrain)
            lgbPred = lgbModel.predict(X_test, 
                num_iteration = lgbModel.best_iteration)
            iterationResults.append(auc(lgbPred, y_test, reorder = True))
        results[i] = pd.Series(iterationResults)
        del X_train, X_test, y_train, y_test
        gc.collect()

    print(testParam)
    print(results)
    results.to_csv('processing/submission9/' + testParam + '.csv', 
        index = False)

#num_boost_round
#run_cv_search('num_boost_round', list(range(100, 2101, 400))) #Best value 100
#run_cv_search('num_boost_round', list(range(50, 501, 25)))#Best value 175

#num_leaves
#run_cv_search('num_leaves', list(range(10, 511, 50)))#Best value 160

#min_data_in_leaf
#run_cv_search('min_data_in_leaf', list(range(100, 10101, 1000)))#Best 100-1000
#run_cv_search('min_data_in_leaf', list(range(50, 1551, 100)))#Best 450

#feature_fraction
#run_cv_search('feature_fraction', [0.6,0.7,0.75,0.8,0.85,0.9,1.0])#Best 0.6
#run_cv_search('feature_fraction', [0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55])#Best 0.35

#bagging_fraction
#run_cv_search('bagging_fraction', [0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])#best 0.5

#bin_construct_sample_cnt
#run_cv_search('bin_construct_sample_cnt', list(range(5000, 30001, 5000)))#No best

#Train model
y = train[target]
train = train.drop(['is_attributed', 'ip'], axis = 1)
lgbTrain = lgb.Dataset(train, y, 
            categorical_feature=categorical, free_raw_data = False)
finalModel = lgb.train(lgbParams, lgbTrain)
finalModel.save_model('processing/submission9/model3M.txt')

