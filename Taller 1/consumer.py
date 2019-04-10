import time
from confluent_kafka import Consumer, KafkaException
import fastavro
import io
from astropy.time import Time
import psycopg2
import psycopg2.extras
import numpy as np
import pandas

def getLastTopic(consumer):
    topics = sorted(consumer.list_topics().topics)
    return topics[-4]

def connectBroker(broker, groupId):
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': groupId,
        'auto.offset.reset': 'earliest'
    })
    topic = getLastTopic(consumer)
    print("Subscribe to: " + topic)
    consumer.subscribe([topic]) #All the partitions assigned to this consumer
    return consumer

def getColumnsName():
    colNames = []
    for col in cur_alerce.description:
        colNames.append(col[0])
    return colNames


def insertAlert(alertId, oid, magpsf, rmsmagpsf, ra, dec, jd, fid, rmsra = 0.09, rmsdec = 0.09):
    line = '''%s, \'%s\', %s, %s, %s, %s, %s, %s, %s''' % (alertId, oid, magpsf, rmsmagpsf, ra, dec, rmsra, rmsdec, jd)
    if fid == 1:
        query = '''INSERT INTO alerts (alertid, oid, magg, rmsg, ra, dec, rmsra, rmsdec, jd) VALUES (%s);'''%(line)
    elif fid == 2:
        query = '''INSERT INTO alerts (alertid, oid, magr, rmsr, ra, dec, rmsra, rmsdec, jd) VALUES (%s);'''%(line)
    print(query)
    return False

def insertObject(oid, data):
    obs = 1
    meanmag = data['magpsf']
    medianmag = data['magpsf']
    minmag = data['magpsf']
    maxmag = data['magpsf']
    rmsmag = 0
    meanra = data['ra']
    meandec = data['dec']
    rmsra = 0
    rmsdec = 0
    deltajd = 0
    lastmag = data['magpsf']
    lastjd = data['jd']
    firstmag = data['magpsf']
    firstjd = data['jd']
    ins = '''\'%s\', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'''%(oid, obs, meanmag, medianmag, maxmag, minmag, rmsmag, meanra, meandec, rmsra, rmsdec, deltajd, lastmag, lastjd, firstmag, firstjd)
    insert = ""
    if data['fid'] == 1:
        insert = '''INSERT INTO objects (oid, nobs, meang, mediang, maxg, ming, rmsg, meanra, meandec, rmsra, rmsdec, deltajd, lastmagg, lastjd, firstmagg, firstjd) VALUES (%s)'''%(ins)
    elif data['fid'] == 2:
        insert = '''INSERT INTO objects (oid, nobs, meanr, medianr, maxr, minr, rmsr, meanra, meandec, rmsra, rmsdec, deltajd, lastmagr, lastjd, firstmagr, firstjd) VALUES (%s)'''%(ins)
    print(insert)
    return True

def isRegisteredObject(oid):
    query = '''SELECT * FROM objects WHERE oid = \'%s\' ;'''%(oid)
    if res:
        return res
    return False

def selectAlerts(oid):
    select = '''SELECT magg, magr, rmsg, rmsr, ra, dec, jd FROM  alerts WHERE oid = \'%s\';'''%(oid)
    colNames = getColumnsName()
    if res:
        return pandas.DataFrame(np.array(res), columns=colNames)
    return False

def updateObject(oid, alerts):
    obs = len(alerts.index)
    alerts = alerts.fillna(0)
    meang = alerts['magg'].mean()
    meanr = alerts['magr'].mean()
    mediang = alerts['magg'].median()
    medianr = alerts['magr'].median()
    maxg = alerts['magg'].max()
    maxr = alerts['magr'].max()
    ming = alerts['magg'].min()
    minr = alerts['magr'].min()
    rmsg = alerts['magg'].std()
    rmsr = alerts['magr'].std()
    meanra = alerts['ra'].mean()
    meandec = alerts['dec'].mean()
    rmsra = alerts['ra'].std()
    rmsdec = alerts['dec'].std()
    deltajd = alerts['jd'].max() - alerts['jd'].min()
    last = alerts.iloc[-1]
    lastmagg = last['magg']
    lastmagr = last['magr']
    lastjd = last['magg']
    # Insert classification
    """
    classrnn
    pclassrnn
    classrf
    pclassrf
    classxmatch
    """
    update = '''UPDATE objects SET nobs=%s, meang=%s, meanr=%s, mediang=%s, medianr=%s, maxg=%s, maxr=%s, ming=%s, minr=%s,rmsg=%s,rmsr=%s, meanra=%s, meandec=%s, rmsra=%s, rmsdec=%s, deltajd=%s, lastmagg=%s, lastmagr=%s, lastjd=%s WHERE oid=\'%s\';'''%(obs, meang, meanr, mediang, medianr, maxg, maxr, ming, minr, rmsg, rmsr, meanra, meandec, rmsra, rmsdec, deltajd, lastmagg, lastmagr, lastjd, oid)
    print(update)
    cur_alerce.execute(update)
    conn_alerce.commit()
    return True

# EXECUTION
# Establish connection to ZTF Alerts
consumer = connectBroker('broker', 'ALeRCE-test-ZTF')
print("Connected at: " + time.strftime("%H:%M:%S"))
# Get stream of data ever. Timer in seconds
timer = 1
name = getLastTopic(consumer)
output = open(name + ".csv", "a")
while True:
    try:
        msg = consumer.poll(timer)
        if msg is None:
            print("None")
            continue
        elif msg.error():
            topic = getLastTopic(consumer)
            if topic == name:
                consumer.subscribe([getLastTopic(consumer)])
                output.write(time.strftime("%H:%M:%S")+",,Subscribe," + topic + "\n")
                time.sleep(3600)
            else:
                output.close()
                name = topic
                output = open(name + ".csv", "a")
            continue
        else:
            # Read AVRO
            bytes_io = io.BytesIO(msg.value())
            reader = fastavro.reader(bytes_io)
            data = reader.next()

            # Set data to variables
            oid = data['objectId']
            data = data['candidate']

            # Insert new alert
            insertAlert(data['candid'], oid, data['magpsf'], data['sigmapsf'], data['ra'], data['dec'], data['jd'], data['fid'])
            t = Time(data['jd'], format="jd")
            # When the alert's oid is known, we update the object
            if isRegisteredObject(oid):
                output.write(time.strftime("%H:%M:%S") + "," + t.isot + ", Old object," + oid + "\n")
                alerts = selectAlerts(oid)
                updateObject(oid, alerts)
            else:
                output.write(time.strftime("%H:%M:%S") + "," + t.isot + ",New object, " + oid + "\n")
                insertObject(oid, data)

    except KeyboardInterrupt:
        print("Interrupted")
        consumer.close()
