

#Inputs used
#0 4750000 a
#4750000 9500000 b
#9500000 14250000 c
#14250000 18790469 d

import MySQLdb
import pandas as pd 
import sys

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(query):
    """runs a mysql query and returns a dataFrame"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    df = pd.DataFrame.from_records(dbFetched)
    return df

def get_prob(numerator, denominator):
    """Returns the probablitity and ensures the correct range of values"""
    prob = round(numerator / denominator, 10)
    if prob > 1: prob = 1
    return prob

#Reading lookup tables and making dicts

ipDF = pd.read_csv('processing/submission7/ip.csv')
pIP = dict(zip(ipDF['train.ip'], ipDF['pClicks']))
pIPa = dict(zip(ipDF['train.ip'], ipDF['pAttributed']))

hourDF = pd.read_csv('processing/submission7/hour.csv')
pHour = dict(zip(hourDF['hour.clicks'], hourDF['pClicks']))
pHourA = dict(zip(hourDF['hour.clicks'], hourDF['pAttributed']))

dayDF = pd.read_csv('processing/submission7/day.csv')
pDay = dict(zip(dayDF['day.clicks'], dayDF['pClicks']))
pDayA = dict(zip(dayDF['day.clicks'], dayDF['pAttributed']))

dayTest = pd.read_csv('processing/submission7/dayTest.csv')
dayTest = dict(zip(dayTest['test.ip'], dayTest['clicks']))

hourTest = pd.read_csv('processing/submission7/hourTest.csv')
#hourTest = dict(zip(hourTest['test.ip'], hourTest['clicks']))
hourDict = {}
for i in range(4, 16):
    temp = hourTest[hourTest['hour'].isin([i])]
    hourDict[i] = dict(zip(temp['test.ip'], temp['clicks']))

hourTest = hourDict


#Running predictor
#startRow = 0
#testRows = 10000#18790469
#letter = 'a'

startRow = int(sys.argv[1])
testRows = int(sys.argv[2])#18790469
letter = sys.argv[3]

subset = 50000
pAttributed = 456846/184903890

#make files
tempDF = pd.DataFrame(columns = ['click_id', 'is_attributed'])
tempDF.to_csv('submissions/submission7IP' + letter + '.csv', \
              index = False)
tempDF.to_csv('submissions/submission7full' + letter + '.csv', \
              index = False)

#making predictions
while (startRow < testRows):
    is_attributedIP = []
    is_attributedFull = [] #just using the new features
    click_id = []

    print(str(startRow) + '/' + str(testRows))
    query = """SELECT click_id, ip, HOUR(click_time) AS hour \
               FROM test LIMIT """ + str(subset) + """ OFFSET """ + \
               str(startRow) + """;"""
    test = run_query(query)

    
    for index, row in test.iterrows():
        numerator, denominator = pAttributed, 1
        click_id.append(row['test.click_id'])

        if row['test.ip'] in pIP:
            numerator = numerator * pIPa[row['test.ip']]
            denominator = denominator * pIP[row['test.ip']]
        is_attributedIP.append(get_prob(numerator,denominator))

        if dayTest[row['test.ip']] in pDayA:
            numerator = numerator * pDayA[dayTest[row['test.ip']]]
            denominator = denominator * pDay[dayTest[row['test.ip']]]

        if hourTest[row['hour']][row['test.ip']] in pHourA:
           numerator *= pHourA[hourTest[row['hour']][row['test.ip']]]
           denominator *= pHour[hourTest[row['hour']][row['test.ip']]]
        is_attributedFull.append(get_prob(numerator,denominator))            


    startRow = startRow + subset

    #creating files
    click_id = pd.Series(click_id)
    is_attributedIP = pd.Series(is_attributedIP)
    is_attributedFull = pd.Series(is_attributedFull)

    ipDF = pd.concat((click_id, is_attributedIP), axis = 1)
    fullDF = pd.concat((click_id, is_attributedFull), axis = 1)

    ipDF.columns = ['click_id', 'is_attributed']
    fullDF.columns = ['click_id', 'is_attributed']

    with open('submissions/submission7IP' + letter + '.csv', 'a') as n:
        ipDF.to_csv(n, header = False, index = False)
    with open('submissions/submission7full'+ letter + '.csv', 'a') as f:
        fullDF.to_csv(f, header = False, index = False)


