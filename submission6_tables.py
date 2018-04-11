# Creating new predictors for naive bayes
# new predictors are all combinations of
# the app, device, os, and channel columns

import itertools
import MySQLdb
import pandas as pd 

trainRows = 184903890 #Rows in test file
trainTable = 'train'

attributed = 456846

#Connecting to MySQL
db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(query):
    """runs a mysql query and returns a dataFrame"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    df = pd.DataFrame.from_records(dbFetched)
    return df

def create_query(cols, attributed):
    """creates a query for MySQL"""
    colsStr = cols[0]
    for i in range(1, len(cols)):
        colsStr = colsStr + ', ' + cols[i]
    query = 'SELECT ' + colsStr + ', COUNT(ip) AS ip_count' + \
            ' FROM ' + trainTable
    if attributed:
        query = query + ' WHERE is_attributed = 1'
    query = query + ' GROUP BY ' + colsStr + ';'
    return query

def row_name(df):
    """creates a unique row name from the data in the row"""
    rowName = []
    gen = df.iterrows()
    for i in range(len(df.index)):
        row = next(gen)
        temp = ''
        for j in row[1]: temp = temp + str(j).zfill(4)
        rowName.append(temp)
    return rowName

#Creating combinations of variables
columns = ['app', 'device', 'os', 'channel']
combinations = list(itertools.combinations(columns, 3)) + \
               list(itertools.combinations(columns, 2))
combinations.append(columns)

#Creating lookup tables
for i in combinations:
    attributedDF = run_query(create_query(i, True))
    attributedDF['rName'] = row_name(attributedDF.drop('ip_count', axis = 1))
    attributedDF['prob'] = attributedDF['ip_count'] / attributed

    totalDF = run_query(create_query(i, False))
    totalDF['rName'] = row_name(totalDF.drop('ip_count', axis = 1))
    totalDF['prob'] = totalDF['ip_count'] / trainRows

    fName = 'processing/submission6/'
    for j in i: fName = fName + j
    totalDF.to_csv(fName + '.csv', index = False)
    attributedDF.to_csv(fName + 'A.csv', index = False)

    print('processed ' + fName)
