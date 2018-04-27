# https://en.wikipedia.org/wiki/Naive_Bayes_classifier
# a basic naive bayes predictor

# input values are the starting row of the test set to use,
# ending row of the test set to use, and an identifier.
# I used letters a-d as the identifiers and ran
# for instances of this script at the same time.

import MySQLdb
import pandas as pd 
import sys

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

def run_query(query):
    """runs a mysql query and returns a dict"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    return dbFetched

#reading tables

def get_dict(tableName, var, p):
    df = pd.read_csv('processing/submission1/' + tableName + '.csv')
    return dict(zip(df[var], df[p]))
    
pAppAt = get_dict('AppAttributed', 'app', 'pApp')
pApp = get_dict('pApp', 'app', 'pApp')
pOSat = get_dict('OSattributed', 'os', 'pOS')
pOS = get_dict('pOS', 'os', 'pOS')
pChannelAt = get_dict('ChannelAttributed', 'channel', 'pChannel')
pChannel = get_dict('pChannel', 'channel', 'pChannel')
pDayAt = get_dict('DayAttributed', 'day', 'pDay')
pDay = get_dict('pDay', 'day', 'pDay')
pHourAt = get_dict('HourAttributed', 'hour', 'pHour')
pHour = get_dict('pHour', 'hour', 'pHour')
pDeviceAt = get_dict('DeviceAttributed', 'device', 'pDevice')
pDevice = get_dict('pDevice', 'device', 'pDevice')

#Running test data

def get_app(appNum):
    """returns probabilities for the app"""
    appValue = 1
    if appNum in pAppAt:
        appValue = pAppAt[appNum]
        appValue = appValue / pApp[appNum]
    return appValue

def get_os(osNum):
    """return probabilities for the os"""
    osValue = 1
    if osNum in pOSat:
        osValue = pOSat[osNum]
        osValue = osValue / pOS[osNum]
    #print('osValue ' + str(osValue))
    return osValue

def get_channel(channelNum):
    """return probabilities for the channel"""
    channelValue = 1
    if channelNum in pChannelAt:
        channelValue = pChannelAt[channelNum]
        channelValue = channelValue / pChannel[channelNum]
    #print('channelValue ' + str(channelValue))
    return channelValue

def get_day(dayNum):
    """return probabilities for the day"""
    dayValue = 1
    if dayNum in pDayAt:
        dayValue = pDayAt[dayNum]
        dayValue = dayValue / pDay[dayNum]
    return dayValue

def get_hour(hourNum):
    """return probabilities for the hour"""
    hourValue = 1
    if hourNum in pHourAt:
        hourValue = pHourAt[hourNum]
        hourValue = hourValue / pHour[hourNum]
    return hourValue

def get_device(deviceNum):
    """return probabilities for the device"""
    deviceValue = 1
    if deviceNum in pDeviceAt:
        deviceValue = pDeviceAt[deviceNum]
        deviceValue = deviceValue / pDevice[deviceNum]
    return deviceValue

startRow = int(sys.argv[1])
testRows = int(sys.argv[2])#18790469
letter = sys.argv[3]
is_attributed = []
click_id = []
subset = 100000
pAttributed = 456846/184903890

#predicting probabliitios
while (startRow < testRows):
    print(testRows - startRow)
    query = """SELECT click_id, app, device, os, channel,\
               HOUR(click_time) AS hour, DAYOFWEEK(click_time) AS day \
               FROM test LIMIT """ + str(subset) + """ OFFSET """ + \
               str(startRow) + """;"""
    test = pd.DataFrame.from_records(run_query(query))
    loop = subset
    if subset > (testRows - startRow):
        loop = testRows - startRow
    for i in range(loop):
        prob = pAttributed * get_app(test['test.app'][i]) * \
        get_os(test['test.os'][i]) * get_channel(test['test.channel'][i]) * \
        get_day(test['day'][i]) * get_hour(test['hour'][i]) * \
        get_device(test['test.device'][i])
        click_id.append(test['test.click_id'][i])
        is_attributed.append(prob)
    startRow = startRow + subset


click_id = pd.Series(click_id)
is_attributed = pd.Series(is_attributed)
probDF = pd.concat((click_id, is_attributed), axis = 1)
probDF.columns = ['click_id', 'is_attributed']
probDF.to_csv('submissions/submission1' + letter + '.csv', index = False)















