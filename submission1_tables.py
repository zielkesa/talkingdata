#Creating the lookup tables for submission 1

import MySQLdb
import pandas as pd 

db = MySQLdb.connect("localhost", "python", "password", "talkingdata")

trainRows = 184903890 #Rows in test file
trainTable = 'train'

attributed = 456846

def run_query(query):
    """runs a mysql query and returns a the result"""
    db.query(query)
    dbResult = db.store_result()
    dbFetched = dbResult.fetch_row(maxrows = 0, how = 2)
    return dbFetched

def write_csv(df, fileName):
    df.to_csv('processing/submission1/' + fileName + '.csv', index = False)

# Total attributed
#query = """SELECT COUNT(is_attributed) AS attributed 
#           FROM train
#           WHERE is_attributed = 1;"""
#attributed = run_query(query)[0]['attributed']
#pAttributed = attributed / trainRows
#print('total attributed: ' + str(attributed))


# app (check to see how many apps there are)
query = """SELECT app, COUNT(app) AS apps_attributed 
           FROM train 
           WHERE is_attributed = 1
           GROUP BY app;"""
pAppAt = pd.DataFrame.from_records(run_query(query))
pAppAt.columns = ['apps_attributed', 'app']
pAppAt['pApp'] = pAppAt['apps_attributed'] / attributed
write_csv(pAppAt, 'AppAttributed')
pAppAt = dict(zip(pAppAt['app'], pAppAt['pApp']))

query = """SELECT app, COUNT(app) AS apps_count 
           FROM train 
           GROUP BY app;"""
pApp = pd.DataFrame.from_records(run_query(query))
pApp.columns = ['apps_count', 'app']
pApp['pApp'] = pApp['apps_count'] / trainRows
write_csv(pApp, 'pApp')
pApp = dict(zip(pApp['app'], pApp['pApp']))

print('got app')


# os (check to see how many os's there are)
query = """SELECT os, COUNT(os) AS os_attributed
           FROM train
           WHERE is_attributed = 1
           GROUP BY os;"""
pOSat = pd.DataFrame.from_records(run_query(query))
pOSat.columns = ['os_attributed', 'os']
pOSat['pOS'] = pOSat['os_attributed'] / attributed
write_csv(pOSat, 'OSattributed')
pOSat = dict(zip(pOSat['os'], pOSat['pOS']))

query = """SELECT os, COUNT(os) AS os_count
           FROM train
           GROUP BY os;"""
pOS = pd.DataFrame.from_records(run_query(query))
pOS.columns = ['os_count', 'os']
pOS['pOS'] = pOS['os_count'] / trainRows
write_csv(pOS, 'pOS')
pOS = dict(zip(pOS['os'], pOS['pOS']))

print('got os')

# Channel
query = """SELECT channel, COUNT(channel) AS channel_attributed
           FROM train
           WHERE is_attributed = 1
           GROUP BY channel;"""
pChannelAt = pd.DataFrame.from_records(run_query(query))
pChannelAt.columns = ['channel_attributed', 'channel']
pChannelAt['pChannel'] = pChannelAt['channel_attributed'] / attributed
write_csv(pChannelAt, 'ChannelAttributed')
pChannelAt = dict(zip(pChannelAt['channel'], pChannelAt['pChannel']))

query = """SELECT channel, COUNT(channel) AS channel_count
           FROM train
           GROUP BY channel;"""
pChannel = pd.DataFrame.from_records(run_query(query))
pChannel.columns = ['channel_count', 'channel']
pChannel['pChannel'] = pChannel['channel_count'] / trainRows
write_csv(pChannel, 'pChannel')
pChannel = dict(zip(pChannel['channel'], pChannel['pChannel']))

print('got channel')

# click_time (split into both day and hour of day (0-23)
#day of week
query = """SELECT DAYOFWEEK(click_time) AS day,
               COUNT(click_time) AS days_attributed
           FROM train
           WHERE is_attributed = 1
           GROUP BY day;"""
pDayAt = pd.DataFrame.from_records(run_query(query))
pDayAt['pDay'] = pDayAt['days_attributed'] / attributed
write_csv(pDayAt, 'DayAttributed')
pDayAt = dict(zip(pDayAt['day'], pDayAt['pDay']))

query = """SELECT DAYOFWEEK(click_time) AS day,
               COUNT(click_time) AS days_count
           FROM train
           GROUP BY day;"""
pDay = pd.DataFrame.from_records(run_query(query))
pDay['pDay'] = pDay['days_count'] / trainRows
write_csv(pDay, 'pDay')
pDay = dict(zip(pDay['day'], pDay['pDay']))

print('got day')

#hour of day
query = """SELECT HOUR(click_time) AS hour,
               COUNT(click_time) AS hour_attributed
           FROM train
           WHERE is_attributed = 1
           GROUP BY hour;"""
pHourAt = pd.DataFrame.from_records(run_query(query))
pHourAt['pHour'] = pHourAt['hour_attributed'] / attributed
write_csv(pHourAt, 'HourAttributed')
pHourAt = dict(zip(pHourAt['hour'], pHourAt['pHour']))

query = """SELECT HOUR(click_time) AS hour,
               COUNT(click_time) AS hour_count
           FROM train
           GROUP BY hour;"""
pHour = pd.DataFrame.from_records(run_query(query))
pHour['pHour'] = pHour['hour_count'] / trainRows
write_csv(pHour, 'pHour')
pHour = dict(zip(pHour['hour'], pHour['pHour']))

print('got hour')

#device
query = """SELECT device, COUNT(device) AS device_attributed
           FROM train
           WHERE is_attributed = 1
           GROUP BY device;"""
pDeviceAt = pd.DataFrame.from_records(run_query(query))
pDeviceAt.columns = ['device_attributed', 'device']
pDeviceAt['pDevice'] = pDeviceAt['device_attributed'] / attributed
write_csv(pDeviceAt, 'DeviceAttributed')
pDeviceAt = dict(zip(pDeviceAt['device'], pDeviceAt['pDevice']))

query = """SELECT device, COUNT(device) AS device_count
           FROM train
           GROUP BY device;"""
pDevice = pd.DataFrame.from_records(run_query(query))
pDevice.columns = ['device_count', 'device']
pDevice['pDevice'] = pDevice['device_count'] / trainRows
write_csv(pDevice, 'pDevice')
pDevice = dict(zip(pDevice['device'], pDevice['pDevice']))

print('got device')
