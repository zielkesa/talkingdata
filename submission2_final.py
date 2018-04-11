# Random forest
# got some code from here: 
#    http://blog.yhat.com/posts/random-forests-in-python.html
#

import numpy as np
import pandas as pd 


# Final Predictions
# mean, all, min
mean, all1, min0, clickId = [], [], [], []

startRow = 0
totalRows = 18790469
section = 100000

dfHeaders = []

while startRow < totalRows:
    allPredsDF = pd.read_csv('processing/submission2/allPreds.csv',
                             skiprows = startRow, nrows = section,
                             header = 0)
    if len(dfHeaders) == 0:
        dfHeaders = allPredsDF.columns
    else:
        allPredsDF.columns = dfHeaders
    startRow = startRow + section
    clickId = clickId + allPredsDF['click_id'].tolist()
    for index, row in allPredsDF.iterrows():
        if index % 10000 == 0:
            print(index)
        rowMean = np.mean(row[1:])
        mean.append(round(rowMean, 5))
        if rowMean < 1:
            min0.append(0)
        else:
            min0.append(1)
        if rowMean > 0:
            all1.append(1)
        else:
            all1.append(0)

meanDF = pd.DataFrame({'click_id' : clickId, 
                       'is_attributed': mean})
meanDF.to_csv('submissions/submission2mean.csv', index = False)

all1DF = pd.DataFrame({'click_id' : clickId, 
                       'is_attributed' : all1})
all1DF.to_csv('submissions/submission2all1.csv', index = False)

min0DF = pd.DataFrame({'click_id' : clickId, 
                       'is_attributed' : min0})
min0DF.to_csv('submissions/submission2min.csv', index = False)




