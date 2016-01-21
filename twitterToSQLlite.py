from twython import TwythonStreamer
import string, json, pprint
import urllib
from os.path import isfile
import pyspatialite.dbapi2 as db
from datetime import datetime
from datetime import date
from time import *
import string, os, sys, subprocess, time
import psycopg2

class MyStreamer(TwythonStreamer):
    
    def on_success(self, data):
        self.output_file = 'geoTweets2.sqlite'
        tweet_lat = 0.0
        tweet_lon = 0.0
        tweet_name = ""
        retweet_count = 0

        if 'id' in data:
            tweet_id = data['id']
        if 'text' in data:
            tweet_text = data['text'].encode('utf-8').replace("'","''").replace(';','')
        else:
            tweet_text = ''
            
        if 'coordinates' in data:    
            geo = data['coordinates']
            if not geo is None:
                latlon = geo['coordinates']
                tweet_lon = latlon[0]
                tweet_lat= latlon[1]
        if 'created_at' in data:
            dt = data['created_at']
            tweet_datetime = datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
        else:
            tweet_datetime = ''

        if 'user' in data:
            users = data['user']
            tweet_name = users['screen_name']
        else:
            tweet_name = ''

        if 'retweet_count' in data:
            retweet_count = data['retweet_count']
                
        if tweet_lat != 0:
            #some elementary output to console
            dataToStore = {
                'id' : tweet_id,
                'name' : tweet_name,
                'time' : tweet_datetime,
                'lat' : tweet_lat,
                'lng' : tweet_lon,
                'text': tweet_text}
            string_to_write = str(tweet_datetime)+", "+str(tweet_lat)+", "+str(tweet_lon)+": "+str(tweet_text)
            print string_to_write
            writeToSQLite(dataToStore,self.output_file)
##            write_tweet(dataToStore,self.output_file)
             
        def on_error(self, status_code, data):
            print "OOPS FOUTJE: " +str(status_code)
            #self.disconnect

##main procedure
def mainStreaming():
    ##codes to access twitter API. 
    APP_KEY = 'WuzvSL1HhEgYgGAlmiOCLYLGC'
    APP_SECRET =  'aAtDzyCNb6lHGhlQtEw3IEnGtYxr9ghDJ6BprxQt4DzvJD4KbK'
    OAUTH_TOKEN =  '21892804-Jf4RT81FAVRo8k5TnV7ZN9w1bfURZ6O1OEgxUcG4Q'
    OAUTH_TOKEN_SECRET = '54T2JpR4CHp09B0cYGXaowkUurCmVXuoBgACGv4RNd2Qs'
    try:
        stream = MyStreamer(APP_KEY, APP_SECRET,OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
        print 'Connecting to twitter: will take a minute'
    except ValueError:
        print 'OOPS! that hurts, something went wrong while making connection with Twitter: '+str(ValueError)
    #global target
    
    
    # Filter based on bounding box see twitter api documentation for more info
    try:
        stream.statuses.filter(locations='3.00,50.00,7.35,53.65')
    except ValueError:
        print 'OOPS! that hurts, something went wrong while getting the stream from Twitter: '+str(ValueError)

def writeToSQLite(data,path):
    exsist = False
    if isfile(path):
        exsist = True
    conn = db.connect(path)
    cur = conn.cursor()
    if not exsist:
        print "===================================================================="
        print "It will take a couple of minutes to generate a new sqlite database!!\n"
        print "                 PLEASE BE PATIENT\n"
        print "===================================================================="
        sql = 'SELECT InitSpatialMetadata()'
        cur.execute(sql)
        sql =  'CREATE TABLE tweets ('
        sql += 'id INTEGER NOT NULL PRIMARY KEY,'
        sql += 'name TEXT,'
        sql += 'time TEXT,'
        sql += 'text TEXT)'
        cur.execute(sql)
        # creating a LINESTRING Geometry column
        sql = "SELECT AddGeometryColumn('tweets', "
        sql += "'geom', 4326, 'POINT', 'XY')"
        cur.execute(sql)
    tweetN = data['id']
    try:
        name = str(data['name'])
    except:
        name = ''
    try:    
        time = str(data['time'])
    except:
        time = ''
    try:        
        text = str(data['text'])
    except:
        text = ''
    try:        
        lat = str(data['lat'])
    except:
        lat = ''
    try:        
        lng = str(data['lng'])
    except:
        lng = ''
    geom = "GeomFromText('POINT("
    geom += lng
    geom += ' '
    geom += lat 
    geom += ")', 4326)"
    sql = "INSERT INTO tweets (id, name, time, text, geom) "
    sql += "VALUES (%d, '%s', '%s', '%s', %s)" % (int(tweetN),
        name,
        time,
        text,
        geom)
    cur.execute(sql)
    conn.commit()
    conn.close()

def tweetsNearRailways():
    path = 'geoTweets2.sqlite'
    conn = db.connect(path)
    cur = conn.cursor()
    sql = "SELECT tweets.name, tweets.text, tweets.geom \
        FROM tweets, railways\
        WHERE st_intersects(railways.geom,st_buffer(tweets.geom,0.0001));"
    rs = cur.execute(sql)
    print "tweets near railroads:"
    for row in rs:
        msg = "==============================================\n"
        msg += "Name : "+row[0]+"\n"
        msg += "Text : "+row[1]
        print msg
        print row[2]
    conn.close()
    
if __name__ == "__main__":
    print "use mainStreaming() to add tweets to the database."
    print "stop by using Ctrl+c"
    print "use tweetsNearRailways() to show the tweets near railways."
    
##    mainStreaming()
    tweetsNearRailways()    
