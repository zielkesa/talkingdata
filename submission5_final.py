# Logistic Regression. Finalizing the submission.

import numpy as np
import pandas as pd 


# Final Predictions
# mean, all, min
mean, maxed, max2, clickId = [], [], [], []

startRow = 0
totalRows = 18790469
section = 100000

dfHeaders = []

while startRow < totalRows:
    allPredsDF = pd.read_csv('processing/submission5/allPreds.csv',
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
            print(str(startRow) + '/' + str(totalRows))
        rowMean = np.mean(row[1:])
        mean.append(round(rowMean, 5))
        maxed.append(round(max(row[1:]), 5))
        rowSum = np.sum(row[1:])
        if rowSum > 0.001:
            max2.append(1)
        else:
            max2.append(0)


meanDF = pd.DataFrame({'click_id' : clickId, 
                       'is_attributed': mean})
meanDF.to_csv('submissions/submission5mean.csv', index = False)

maxDF = pd.DataFrame({'click_id' : clickId, 
                      'is_attributed' : maxed})
maxDF.to_csv('submissions/submission5max.csv', index = False)

max2DF = pd.DataFrame({'click_id': clickId,
                       'is_attributed' : max2})
max2DF.to_csv('submissions/submission5max2.csv', index = False)

