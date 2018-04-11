# Naive Bayes predictor 

# input values are the starting row of the test set to use,
# ending row of the test set to use, and an identifier.
# I used letters a-d as the identifiers and ran
# for instances of this script at the same time.

#Inputs used
#0 4750000 a
#4750000 9500000 b
#9500000 14250000 c
#14250000 18790469 d


import itertools
import MySQLdb
import pandas as pd 
import re
import sys

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(query):
    """runs a mysql query and returns a dataFrame"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    df = pd.DataFrame.from_records(dbFetched)
    df.columns = [re.sub('test.', '', x) for x in df.columns]
    return df

def row_content(subDF):
    """creates a new column based on the combined features"""
    combined = []
    gen = subDF.iterrows()
    for i in range(len(subDF.index)):
        row = next(gen)
        temp = ''
        for j in row[1]: temp = temp + str(j).zfill(4)
        combined.append(temp)
    return combined

def create_features(df):
    """creates the combination features"""
    for i in combinations:
        cName = ''
        for j in i: cName = cName + j
        df[cName] = row_content(df[list(i)])
    return df

def read_tables():
    """reads in look up tables, returns a dict of dicts"""
    lookUP = {}
    for i in comboNames:
        tempDF = pd.read_csv('processing/submission6/' + i + '.csv')
        lookUP[i] = dict(zip(tempDF['rName'], tempDF['prob']))

        tempDF = pd.read_csv('processing/submission6/' + i + 'A.csv')
        lookUP[i + 'A'] = dict(zip(tempDF['rName'], tempDF['prob']))

    for i in singles:
        tempDF = pd.read_csv('processing/submission6/' + i + '.csv')
        lookUP[i] = dict(zip(tempDF['rName'], tempDF['prob']))

        tempDF = pd.read_csv('processing/submission6/' + i + 'A.csv')
        lookUP[i + 'A'] = dict(zip(tempDF['rName'], tempDF['prob']))
    return lookUP

def get_prob(numerator, denominator):
    """Returns the probablitity and ensures the correct range of values"""
    prob = round(numerator / denominator, 10)
    if prob > 1: prob = 1
    return prob
         

#Creating lists of variables
columns = ['app', 'device', 'os', 'channel']
combinations = list(itertools.combinations(columns, 3)) + \
               list(itertools.combinations(columns, 2))
combinations.append(columns)
comboNames = []
for i in combinations:
    temp = ''
    for j in i: temp = temp + j
    comboNames.append(temp)

singles = ['ip', 'hour'] + columns

#Running predictor
#startRow = 0
#testRows = 18790469

startRow = int(sys.argv[1])
testRows = int(sys.argv[2])#18790469
letter = sys.argv[3]

subset = 100000
pAttributed = 456846/184903890



lookUP = read_tables()

#make files
tempDF = pd.DataFrame(columns = ['click_id', 'is_attributed'])
tempDF.to_csv('submissions/submission6new' + letter + '.csv', \
              index = False)
tempDF.to_csv('submissions/submission6full' + letter + '.csv', \
              index = False)

while (startRow < testRows):
    is_attributed = []
    is_attributedNew = [] #just using the new features
    click_id = []

    print(str(startRow) + '/' + str(testRows))
    query = """SELECT click_id, ip, app, device, os, channel,\
               HOUR(click_time) AS hour, DAYOFWEEK(click_time) AS day \
               FROM test LIMIT """ + str(subset) + """ OFFSET """ + \
               str(startRow) + """;"""
    test = run_query(query)
    test = create_features(test)

    loop = subset
    if subset > (testRows - startRow):
        loop = testRows - startRow
    
    for i in range(loop):
        numerator, denominator = pAttributed, 1
        click_id.append(test['click_id'][i])

        for j in comboNames:
            key = int(test[j][i])
            if key in lookUP[j + 'A'].keys():
                numerator = numerator * lookUP[j + 'A'][key]
                denominator = denominator * lookUP[j][key]
        is_attributedNew.append(get_prob(numerator,denominator))

        for j in singles:
            key = int(test[j][i])
            if key in lookUP[j + 'A'].keys():
                numerator = numerator * lookUP[j + 'A'][key]
                denominator = denominator * lookUP[j][key]
        is_attributed.append(get_prob(numerator,denominator))

    startRow = startRow + subset

    #creating files
    click_id = pd.Series(click_id)
    is_attributed = pd.Series(is_attributed)
    is_attributedNew = pd.Series(is_attributedNew)

    newDF = pd.concat((click_id, is_attributedNew), axis = 1)
    fullDF = pd.concat((click_id, is_attributed), axis = 1)

    newDF.columns = ['click_id', 'is_attributed']
    fullDF.columns = ['click_id', 'is_attributed']

    with open('submissions/submission6new' + letter + '.csv', 'a') as n:
        newDF.to_csv(n, header = False, index = False)
    with open('submissions/submission6full'+ letter + '.csv', 'a') as f:
        fullDF.to_csv(f, header = False, index = False)

