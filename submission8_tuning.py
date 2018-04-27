# Tuning parameters for a gradient boosting model
# Comes from this post
# https://www.analyticsvidhya.com/blog/2016/02/complete-guide-parameter-tuning-gradient-boosting-gbm-python/

#Import libraries:
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier  #GBM algorithm
from sklearn import cross_validation, metrics   #Additional scklearn functions
from sklearn.grid_search import GridSearchCV   #Perforing grid search

import matplotlib.pylab as plt
#%matplotlib inline
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 12, 4

train = pd.read_csv('processing/sample/subTrain.csv')
print("read in train")
target = 'train.is_attributed'

#Final parameters found
n_est = 500
max_dep = 10
min_samp = 20000
min_samp_leaf = 2600
max_feat = 5
sub_samp = 0.85


def modelfit(alg, dtrain, predictors, performCV=True, printFeatureImportance=True, cv_folds=5):
    #Fit the algorithm on the data
    alg.fit(dtrain[predictors], dtrain[target])
        
    #Predict training set:
    dtrain_predictions = alg.predict(dtrain[predictors])
    dtrain_predprob = alg.predict_proba(dtrain[predictors])[:,1]
    
    #Perform cross-validation:
    if performCV:
        cv_score = cross_validation.cross_val_score(alg, dtrain[predictors], 
                   dtrain[target], cv=cv_folds, scoring='roc_auc')
    
    #Print model report:
    print("\nModel Report")
    print("Accuracy : %.4g" % metrics.accuracy_score(dtrain[target].values, dtrain_predictions))
    print("AUC Score (Train): %f" % metrics.roc_auc_score(dtrain[target], dtrain_predprob))
    
    if performCV:
        print("CV Score : Mean - %.7g | Std - %.7g | Min - %.7g | Max - %.7g" % (np.mean(cv_score),np.std(cv_score),np.min(cv_score),np.max(cv_score)))
        
    #Print Feature Importance:
    if printFeatureImportance:
        feat_imp = pd.Series(alg.feature_importances_, predictors).sort_values(ascending=False)
        feat_imp.plot(kind='bar', title='Feature Importances')
        plt.ylabel('Feature Importance Score')

#Choose all predictors except target 
predictors = [x for x in train.columns if x not in [target]]
gbm0 = GradientBoostingClassifier(random_state=10, verbose = 1)
modelfit(gbm0, train, predictors)

#tuning n_estimators 
param_test1 = {'n_estimators':list(range(200, 500, 50))}
gsearch1 = GridSearchCV(estimator=GradientBoostingClassifier(learning_rate=0.1, 
                                  min_samples_split=5000, min_samples_leaf=50,
                                  max_depth=7, max_features='sqrt', 
                                  subsample=0.8,random_state=10), 
                                  param_grid = param_test1, scoring='roc_auc',
                                  n_jobs=6,iid=False, cv=5, verbose = 2)
gsearch1.fit(train[predictors],train[target])
print(gsearch1.grid_scores_, gsearch1.best_params_, gsearch1.best_score_)





#min_samples_split and max_depth
param_test2 = {'max_depth':list(range(10,19,4)), 
               'min_samples_split':list(range(10000,20001,2000))}
gsearch2 = GridSearchCV(estimator=GradientBoostingClassifier(learning_rate=0.1, 
                        n_estimators=n_est, max_features='sqrt', subsample=0.8,
                        random_state=10), 
                        param_grid = param_test2, scoring='roc_auc', n_jobs=6, 
                        iid=False, cv=5, verbose = 2)
gsearch2.fit(train[predictors],train[target])
print(gsearch2.grid_scores_, gsearch2.best_params_, gsearch2.best_score_)

#min_samples_leaf
param_test3 = {'min_samples_leaf':list(range(100,10001,500))}
gsearch3 = GridSearchCV(estimator=GradientBoostingClassifier(learning_rate=0.1, 
                        n_estimators=n_est, max_depth=max_dep,
                        min_samples_split = min_samp,
                        max_features='sqrt', subsample=0.8, random_state=10), 
                        param_grid = param_test3, scoring='roc_auc', n_jobs=6, 
                        iid=False, cv=5)
gsearch3.fit(train[predictors],train[target])
print(gsearch3.grid_scores_, gsearch3.best_params_, gsearch3.best_score_)

#max_features
param_test4 = {'max_features':list(range(2,7,1))}
gsearch4 = GridSearchCV(estimator=GradientBoostingClassifier(learning_rate=0.1,
                        n_estimators=n_est,max_depth=max_dep, 
                        min_samples_split=min_samp, 
                        min_samples_leaf=min_samp_leaf, 
                        subsample=0.8, random_state=10),
                        param_grid = param_test4, scoring='roc_auc', n_jobs=6, 
                        iid=False, cv=5, verbose = 2)
gsearch4.fit(train[predictors],train[target])
print(gsearch4.grid_scores_, gsearch4.best_params_, gsearch4.best_score_)

#subsample
param_test5 = {'subsample':[0.6,0.7,0.75,0.8,0.85,0.9]}
gsearch5 = GridSearchCV(estimator=GradientBoostingClassifier(learning_rate=0.1,
                        n_estimators=n_est,max_depth=max_dep, 
                        min_samples_split=min_samp, 
                        min_samples_leaf=min_samp_leaf, 
                        random_state=10, max_features=max_feat),
                        param_grid = param_test5, scoring='roc_auc', n_jobs=6, 
                        iid=False, cv=5, verbose = 2)
gsearch5.fit(train[predictors],train[target])
print(gsearch5.grid_scores_, gsearch5.best_params_, gsearch5.best_score_)

#Full tuned model

gbm_tuned_1 = GradientBoostingClassifier(learning_rate=0.1, 
                                         n_estimators=n_est,max_depth=max_dep, 
                                         min_samples_split=min_samp, 
                                         min_samples_leaf=min_samp_leaf, 
                                         subsample=sub_samp, random_state=10, 
                                         max_features=max_feat,
                                         verbose = 1)
modelfit(gbm_tuned_1, train, predictors)
