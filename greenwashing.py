# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 11:59:29 2023

@author: felix
"""


import sqlite3
import datetime
from twarc import Twarc2, expansions
import json
import config
import matplotlib.pyplot as plt
import pytz
import time
#import seaborn as sns
import pandas as pd

class Tweet:
    def __init__(self, json_str=None, corporation="", sqlite_row=None):
        if json_str is not None:
            self.tweet_id = json_str["id"]
            self.retweeted_tweet_id = 0
            try:
                if json_str["referenced_tweets"][0]["type"] == "retweeted":
                    self.retweeted_tweet_id = json_str["referenced_tweets"][0]["id"]
            except:
                pass
            
            self.conservation_id = json_str["conversation_id"]
            self.author_id = json_str["author_id"]
            self.date_posted = datetime.datetime.strptime(json_str["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ')
            self.corporation = corporation
            self.tweet_content = json_str["text"]
            self.author = json_str["author"]["name"]
            try:
                self.location = json_str["author"]["location"]
            except KeyError:
                self.location = "N/A"
            self.retweet_count = json_str["public_metrics"]["retweet_count"]
            self.reply_count = json_str["public_metrics"]["reply_count"]
            self.like_count = json_str["public_metrics"]["like_count"]
            self.quote_count = json_str["public_metrics"]["quote_count"]
            self.impression_count = json_str["public_metrics"]["impression_count"]
            self.follower_count = json_str["author"]["public_metrics"]["followers_count"]
        elif sqlite_row is not None:
            self.tweet_id = sqlite_row[0]
            self.retweeted_tweet_id = sqlite_row[1]
            self.conservation_id = sqlite_row[2]
            self.author_id = sqlite_row[3]
            
            time_offset = time.timezone
            if time_offset > 0:
                self.date_posted = datetime.datetime.strptime(sqlite_row[4] + f" +{str(int(time_offset/3600 * 100)).zfill(4)}", "%Y-%m-%d %H:%M:%S %z")
            else:
                self.date_posted = datetime.datetime.strptime(sqlite_row[4] + f" -{str(int(-time_offset/3600 * 100)).zfill(4)}", "%Y-%m-%d %H:%M:%S %z")
            self.corporation = sqlite_row[5]
            self.tweet_content = sqlite_row[6]
            self.author = sqlite_row[7]
            self.location = sqlite_row[8]
            self.retweet_count = sqlite_row[9]
            self.reply_count = sqlite_row[10]
            self.like_count = sqlite_row[11]
            self.quote_count = sqlite_row[12]
            self.impression_count = sqlite_row[13]
            self.follower_count = sqlite_row[14]
            
    
    def tweet2str(self, long=False):
        long_form = f'''VALUES({self.tweet_id}, {self.conservation_id}, {self.author_id}, {self.date_posted},
    {self.corporation}, {self.tweet_content}, {self.author})'''
        short_form = f"{self.tweet_id}, {self.date_posted}, {self.author}, {self.impression_count}"
        if long:
            return long_form
        else:
            return short_form
    
    def getTweetSubmission(self):
        return (self.tweet_id, self.retweeted_tweet_id, self.conservation_id, self.author_id, self.date_posted, self.corporation,
                self.tweet_content, self.author, self.location,
                self.retweet_count, self.reply_count, self.like_count, self.quote_count, self.impression_count, self.follower_count)
    
    

class GreenwashingDB:
    
    
    def __init__(self, path):
        print(f"Establishing connection to {path}")
        self.conn = sqlite3.connect(path)
        self.table_name = None
        
    def setTable(self, table_name):
        self.table_name = table_name
        
    def createTable(self, table_name):
        try:
            self.table_name = table_name
            sqlite_create_table_query = f'''CREATE TABLE {table_name} (
            tweet_id INTEGER PRIMARY KEY, 
            retweeted_tweet_id INTEGER, 
            conservation_id INTEGER, 
            author_id INTEGER, 
            date_posted datetime, 
            corporation TEXT NOT NULL, 
            tweet_content TEXT NOT NULL, 
            author TEXT NOT NULL, 
            location TEXT, 
            retweet_count INTEGER, 
            reply_count INTEGER, 
            like_count INTEGER, 
            quote_count INTEGER, 
            impression_count INTEGER, 
            follower_count INTEGER);'''
        
            cursor = self.conn.cursor()
            cursor.execute(sqlite_create_table_query)
            self.conn.commit()
            print("SQLite table created")
        
            #cursor.close()
        
        except sqlite3.Error as error:
            print("Note: ", error)
            
        sql_query = """SELECT name FROM sqlite_master  
  WHERE type='table';"""
  
        cursor.execute(sql_query)
            
        print(f"Current tables: {cursor.fetchall()}")

                
    def close(self):
        self.conn.close()
        
    def saveTweet(self, name, tweet):
        
                    
        # TODO: only add if not exists
        
        try:
            sql = f''' INSERT INTO {self.table_name} (tweet_id, retweeted_tweet_id, conservation_id, author_id, date_posted, 
            corporation, tweet_content, author, location, retweet_count, reply_count, like_count, 
            quote_count, impression_count, follower_count) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            
            cursor = self.conn.cursor()
            cursor.execute(sql, tweet.getTweetSubmission())
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass
            #print("Note: ", error)
            
    def listTable(self):
        cursor = self.conn.cursor()
        cursor.execute(f"select * from {self.table_name}")
        results = cursor.fetchall()
        print("\n==================================\n")
        for result in results:
            print(result)
            print("\n==================================\n")
            
            
    def loadTweets(self, sql="", args=None):
        cursor = self.conn.cursor()

        sqlite_select_query = f"""SELECT * from {self.table_name}""" + " " + sql
        if args is None:
            cursor.execute(sqlite_select_query)
        else:
            cursor.execute(sqlite_select_query, args)
        records = cursor.fetchall()
        
        tweets = [Tweet(sqlite_row=row) for row in records]
        
        return tweets

    
    




class GreenwashingMainObj:
    
    def __init__(self):
        self.client = None
        self.db = None
    
    
    def setBearerToken(self, bt):
        self.client = Twarc2(bearer_token=bt)
        
    def setDB(self, myDB):
        self.db = myDB
        
        
    def fetchTweets(self, tweetfilter="", name="", mention="", startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc)):
        
        print(f"Fetching Tweets for {mention} back to {startdate}")
        
        
        query = f"{mention} ({tweetfilter})"
        #query = f"@tacobell @yumbrands #greenwashing"
        
        search_results = self.client.search_all(query=query, max_results=100, start_time=startdate)
        
        for page in search_results:

            result = expansions.flatten(page)
          

            for tweet in result:
                
                # print(json.dumps(tweet))
                # print("===================================================")
                
            
                tweetObj = Tweet(json_str=tweet, corporation=name)
                #print(tweetObj.tweet2str())
                self.db.saveTweet(name, tweetObj)
                
                # grab main tweet from retweets in case the API is buggy
                if int(tweetObj.retweeted_tweet_id) > 0:
                    lookup_alt = self.client.tweet_lookup(tweet_ids=[str(tweetObj.retweeted_tweet_id)])
                    for page_alt in lookup_alt:
                        result_alt = expansions.flatten(page_alt)
                        for tweet_alt in result_alt:
                            tweetObj_alt = Tweet(json_str=tweet_alt, corporation=name)
                            self.db.saveTweet(name, tweetObj_alt)
                
    def printSingleTweets(self, tweet_ids):
        lookup = self.client.tweet_lookup(tweet_ids=tweet_ids)
        for page in lookup:
            # The Twitter API v2 returns the Tweet information and the user, media etc.  separately
            # so we use expansions.flatten to get all the information in a single JSON
            result = expansions.flatten(page)
            for tweet in result:
                # Here we are printing the full Tweet object JSON to the console
                print(json.dumps(tweet))
                
                
class GreenwashingPlots:
                
    def __init__(self):
        self.db = None
        
    def setDB(self, myDB):
        self.db = myDB
        
        
    def plotViewsVSTime(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        
        tweets = self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=("yumbrands", startdate, enddate,))
        
        print(f"Tweets matching search: {len(tweets)}")
        
        # for tweet in tweets:
        #     print(tweet.tweet2str())
        
        tweet_dates = [tweet.date_posted for tweet in tweets]
        
        tweet_seconds = [(tweet_date - startdate).total_seconds() for tweet_date in tweet_dates]
        
        plt.hist(tweet_seconds, bins = 80)
        #sns.distplot(tweet_seconds, kde_kws={'bw':100000})
        plt.xlim(0, (enddate-startdate).total_seconds())
        plt.show()
        
class GreenwashingExcel:
    
    def __init__(self, path):
       self.db = None
       self.path = path
        
    def setDB(self, myDB):
        self.db = myDB
        
    def writeTweets(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        df = pd.DataFrame(columns=['Company','Account','Date','Cmltv_GW_Mentions','Annual_GW_Mentions','Event_Peak','Tweet_Text', 'Tweet_Author', 'Retweets_Count', 'Impression'])
        tweets = self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=("yumbrands", startdate, enddate,))
        
        for tweet in tweets:
            rt = tweet.retweet_count
            if tweet.retweeted_tweet_id > 0:
                rt = -1
                
            dt = tweet.date_posted.replace(tzinfo=None)
            tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Tweet_Author':tweet.author,
                       'Date':dt, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count}
            df = df.append(tweetInfo, ignore_index=True)
        
        df.to_excel(self.path)
        
        
    def writeUniqueTweets(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        df = pd.DataFrame(columns=['Company','Account','Date','Cmltv_GW_Mentions','Annual_GW_Mentions','Event_Peak','Tweet_Text', 'Tweet_Author', 'Retweets_Count', 'Impression'])
        tweets = self.db.loadTweets("WHERE retweeted_tweet_id = ? AND corporation = ? AND date_posted > ? AND date_posted < ?", args=(0, "yumbrands", startdate, enddate,))
        
        for tweet in tweets:
            rt = tweet.retweet_count
            dt = tweet.date_posted.replace(tzinfo=None)
            tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Tweet_Author':tweet.author,
                       'Date':dt, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count}
            df = df.append(tweetInfo, ignore_index=True)
        
        df.to_excel(self.path)
        
    

        