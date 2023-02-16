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
import numpy as np

class Tweet:
    def __init__(self, json_str=None, corporation="", sqlite_row=None, responses_db=False):
        
        
        if json_str is not None:
            self.tweet_id = json_str["id"]
            self.retweeted_tweet_id = 0
            try:
                if json_str["referenced_tweets"][0]["type"] == "retweeted":
                    self.retweeted_tweet_id = json_str["referenced_tweets"][0]["id"]
            except:
                pass
            self.responded_tweet_id = 0
            self.responded_author_id = 0
            try:
                for i in range(len(json_str["referenced_tweets"])):
                    if json_str["referenced_tweets"][i]["type"] == "replied_to":
                        self.responded_tweet_id = json_str["referenced_tweets"][i]["id"]
                        self.responded_author_id = json_str["in_reply_to_user_id"]
            except:
                pass
            
            self.conservation_id = json_str["conversation_id"]
            self.author_id = json_str["author_id"]
            self.date_posted = datetime.datetime.strptime(json_str["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ')
            self.corporation = corporation
            self.tweet_content = json_str["text"]
            self.author = json_str["author"]["name"]
            self.authorhandle = json_str["author"]["username"]
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
            self.responded_tweet_id = sqlite_row[1]
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
            self.authorhandle = sqlite_row[15]
            
            if responses_db:
                self.responded_author_id = sqlite_row[16]
            
    
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
                self.retweet_count, self.reply_count, self.like_count, self.quote_count, self.impression_count, self.follower_count, self.authorhandle)
    def getResponseSubmission(self):
        return (self.tweet_id, self.responded_tweet_id, self.conservation_id, self.author_id, self.date_posted, self.corporation,
                self.tweet_content, self.author, self.location,
                self.retweet_count, self.reply_count, self.like_count, self.quote_count, self.impression_count, self.follower_count, self.authorhandle, self.responded_author_id)
    
class CorporationDB:
    
    
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
            corporation TEXT NOT NULL, 
            tweet_date datetime, 
            tweet_date_end datetime,
            tweet_volume INTEGER,
            fetch_date datetime);'''
        
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
        
    def saveDatapoint(self, name, tweet_date, tweet_date_end, volume, current_date):
        
        try:
            sql = f''' INSERT INTO {self.table_name} (corporation, tweet_date, tweet_date_end, tweet_volume, fetch_date) VALUES(?, ?, ?, ?, ?);'''
            
            cursor = self.conn.cursor()
            cursor.execute(sql, (name, tweet_date, tweet_date_end, volume, current_date))
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
            
            
    def loadValues(self, sql="", args=None):
        cursor = self.conn.cursor()

        sqlite_select_query = f"""SELECT * from {self.table_name}""" + " " + sql
        if args is None:
            cursor.execute(sqlite_select_query)
        else:
            cursor.execute(sqlite_select_query, args)
        records = cursor.fetchall()
        
        values = [[row[1], row[2], row[3]] for row in records]
        
        return values
    
    
    def loadTweetCounts(self, sql="", args=None, corporation = ""):
        cursor = self.conn.cursor()
        
        
        # pull latest date for corporation
        cursor.execute(f"""SELECT * from {self.table_name} WHERE corporation = ?""", [corporation,])
        records = cursor.fetchall()
        fetchdates = [datetime.datetime.strptime(row[-1], '%Y-%m-%d %H:%M:%S.%f') for row in records]
        if len(fetchdates) > 0:
            fetchdate_latest = max(fetchdates)
            
            
            if sql == "":
                sql = "WHERE fetch_date = ?"
                args = (fetchdate_latest,)
            else:
                sql += " AND fetch_date = ?"
                args = args + (fetchdate_latest,)
    
            sqlite_select_query = f"""SELECT * from {self.table_name}""" + " " + sql
            
            
            if args is None:
                cursor.execute(sqlite_select_query)
            else:
                cursor.execute(sqlite_select_query, args)
            records = cursor.fetchall()
            
            tweet_count = np.sum([row[3] for row in records])

            return records, tweet_count
        else:
            return [], 0

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
            follower_count INTEGER,
            authorhandle TEXT NOT NULL);'''
        
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
        
    def printQuarterly(self, corporation):
        #print(f"{corporation}")
        
        y1 = config.START_DATE.year
        y2 = config.END_DATE.year
        
        quarterly_tweets = []
        
        for y in range(y1, y2):
            
            for (yy1, dd1, yy2, dd2) in [(y, 1, y, 4), (y, 4, y, 7),(y,7,y,10),(y,10,y+1,1)]:
            
                d1 = datetime.datetime(yy1, dd1, 1, 0, 0, 0, 0, datetime.timezone.utc)
                d2 = datetime.datetime(yy2, dd2, 1, 0, 0, 0, 0, datetime.timezone.utc)
                
                quarterly_responses = len(self.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=(corporation,d1,d2)))
            
                quarterly_tweets.append(quarterly_responses)
        
        #print(f"{corporation} ,{suffering}")
        
        return quarterly_tweets
    
    def printQuarterlyImpressions(self, corporation):
        #print(f"{corporation}")
        
        y1 = config.START_DATE.year
        y2 = config.END_DATE.year
        
        quarterly_tweets = []
        
        for y in range(y1, y2):
            
            for (yy1, dd1, yy2, dd2) in [(y, 1, y, 4), (y, 4, y, 7),(y,7,y,10),(y,10,y+1,1)]:
            
                d1 = datetime.datetime(yy1, dd1, 1, 0, 0, 0, 0, datetime.timezone.utc)
                d2 = datetime.datetime(yy2, dd2, 1, 0, 0, 0, 0, datetime.timezone.utc)
                
                quarterly_responses = self.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=(corporation,d1,d2))
                impressions = 0
                for tweet in quarterly_responses:
                    impressions += tweet.follower_count
                
            
                quarterly_tweets.append(impressions)
        
        #print(f"{corporation} ,{suffering}")
        
        return quarterly_tweets
        
    def saveTweet(self, name, tweet):
        

        try:
            sql = f''' INSERT INTO {self.table_name} (tweet_id, retweeted_tweet_id, conservation_id, author_id, date_posted, 
            corporation, tweet_content, author, location, retweet_count, reply_count, like_count, 
            quote_count, impression_count, follower_count, authorhandle) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            
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
    
class GreenwashingResponsesDB:
    
    
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
            responded_tweet_id INTEGER, 
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
            follower_count INTEGER,
            authorhandle TEXT NOT NULL,
            responded_author_id INTEGER);'''
        
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
        

        try:
            sql = f''' INSERT INTO {self.table_name} (tweet_id, responded_tweet_id, conservation_id, author_id, date_posted, 
            corporation, tweet_content, author, location, retweet_count, reply_count, like_count, 
            quote_count, impression_count, follower_count, authorhandle, responded_author_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            
            cursor = self.conn.cursor()
            cursor.execute(sql, tweet.getResponseSubmission())
            #print(tweet.getResponseSubmission())
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
        
        tweets = [Tweet(sqlite_row=row,responses_db=True) for row in records]
        
        return tweets

    
    

class CorporationInfoMain:
    
    def __init__(self):
        self.client = None
        self.db = None
        self.current_date = datetime.datetime.now()
    
    
    def setBearerToken(self, bt):
        self.client = Twarc2(bearer_token=bt)
        
    def setDB(self, myDB):
        self.db = myDB
        
        
    def fetchTweets(self, name="", mention="", startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), enddate=datetime.datetime(2023, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)):
        
        print(f"Fetching All Tweets for {mention} back to {startdate}")
        
        query = f"{mention} -is:retweet -is:reply"
        
        y1 = startdate.year
        #y2 = datetime.date.today().year
        y2 = enddate.year
        
        for year in range(y1, y2):
        
            search_results = self.client.counts_all(query=query, start_time=datetime.datetime(year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc), end_time=datetime.datetime(year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc))
            
            #count = 0
            
            
            cnt = 0
            for page in search_results:
                #count += int(json.loads(json.dumps(page))["meta"]["total_tweet_count"])
                
                cnt += 1
                
                # if cnt > 3:
                #     break
                
                entry_num = len(json.loads(json.dumps(page))["data"])
                
                count = int(json.loads(json.dumps(page))["meta"]["total_tweet_count"])
                count_date = json.loads(json.dumps(page))["data"][0]["start"]
                count_date_end = json.loads(json.dumps(page))["data"][entry_num - 1]["start"]
                
                print(count, count_date, count_date_end)
                
                self.db.saveDatapoint(name, count_date, count_date_end, count, self.current_date)
                

        
   


class GreenwashingMainObj:
    
    def __init__(self):
        self.client = None
        self.db = None
        self.db_responses = None
    
    
    def setBearerToken(self, bt):
        self.client = Twarc2(bearer_token=bt)
        
    def setDB(self, myDB):
        self.db = myDB
        
    def setDBresponses(self, myDB):
        self.db_responses = myDB
        
        
    def fetchTweets(self, tweetfilter="", name="", mention="", startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), enddate=datetime.datetime(2023, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)):
        
        print(f"Fetching GW Tweets for {mention} back to {startdate}")
        

        query = f"{mention} ({tweetfilter}) -is:reply"
        #query = f"{mention} ({tweetfilter})"
        #query = f"@tacobell @yumbrands #greenwashing"
        
        search_results = self.client.search_all(query=query, max_results=100, start_time=startdate, end_time=enddate)
        
        for page in search_results:

            result = expansions.flatten(page)
            
            print(f'Fetching {len(result)} tweets...')
          

            for tweet in result:
                
                # print(json.dumps(tweet))
                # print("===================================================")
                
            
                tweetObj = Tweet(json_str=tweet, corporation=name)
                #print(tweetObj.tweet2str())
                self.db.saveTweet(name, tweetObj)
                
                # TODO: move this somewhere else...
                # # grab main tweet from retweets in case the API is buggy
                # if int(tweetObj.retweeted_tweet_id) > 0:
                #     lookup_alt = self.client.tweet_lookup(tweet_ids=[str(tweetObj.retweeted_tweet_id)])
                #     for page_alt in lookup_alt:
                #         result_alt = expansions.flatten(page_alt)
                #         for tweet_alt in result_alt:
                #             tweetObj_alt = Tweet(json_str=tweet_alt, corporation=name)
                #             self.db.saveTweet(name, tweetObj_alt)
                
    def printSingleTweets(self, tweet_ids):
        lookup = self.client.tweet_lookup(tweet_ids=tweet_ids)
        for page in lookup:
            # The Twitter API v2 returns the Tweet information and the user, media etc.  separately
            # so we use expansions.flatten to get all the information in a single JSON
            result = expansions.flatten(page)
            for tweet in result:
                # Here we are printing the full Tweet object JSON to the console
                print(json.dumps(tweet))
                
    def searchResponses(self, name="", mention="", startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), enddate=datetime.datetime(2023, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)):
        print(f"Fetching Responses for {mention} back to {startdate}")
        # read from db
        
        tweets = self.db.loadTweets("WHERE retweeted_tweet_id = ? AND corporation = ? AND date_posted > ? AND date_posted < ?", args=(0, name, startdate, enddate,))
        tweet_authors = np.array([tweet.authorhandle for tweet in tweets])
        tweet_ids = np.array([tweet.tweet_id for tweet in tweets])
        
        print(f"Loaded {len(tweets)} unique GW tweets")
        
        # split requests into chunks of 100
        if len(tweet_authors) > 0:
            CHUNK_LEN = 20
            tweet_author_chunks = np.array_split(tweet_authors, (len(tweet_authors)//CHUNK_LEN + 1))
            tweet_id_chunks = np.array_split(tweet_ids, (len(tweet_ids)//CHUNK_LEN + 1))
            for i in range(len(tweet_author_chunks)):
                # # construct filter
                # query = f"from:{mention[1:]} is:reply ("
                # for c in chunk:
                #     query += '"@{c}" OR '
                # query = query[:-4]
                # query += ")"
                
                query = f"from:{mention[1:]} ("
                
                for j in range(len(tweet_author_chunks[i])):
                    author = tweet_author_chunks[i][j]
                    twid = tweet_id_chunks[i][j]
                    if twid > 0:
                        query += f'conversation_id:{twid} OR '
                    
                query = query[:-4]
                query += ")"
                
                #print(query)
                
                search_results = self.client.search_all(query=query, max_results=100, start_time=startdate, end_time=enddate)
                print(f"request {i+1}/{len(tweet_author_chunks)}...")
                for page in search_results:
                    result = expansions.flatten(page)
                    print(f'Fetching {len(result)} responses...')
                    for tweet in result:
                        
                        tweetObj = Tweet(json_str=tweet, corporation=name)
                        self.db_responses.saveTweet(name, tweetObj)
                        
                        # print("\n\n=============================")
                        # print(json.dumps(tweet))
                        # print("=============================\n\n")
                        
                        
    def searchResponsesFast(self, name="", mention="", startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), enddate=datetime.datetime(2023, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)):
        print(f"Fetching Responses for {mention} back to {startdate}")
        # read from db
        
        tweets = self.db.loadTweets("WHERE retweeted_tweet_id = ? AND corporation = ? AND date_posted > ? AND date_posted < ?", args=(0, name, startdate, enddate,))
        tweet_authors = np.unique(np.array([tweet.authorhandle for tweet in tweets]))
        
        print(f"Loaded {len(tweets)} unique GW tweets")
        
        # split requests into chunks of 100
        if len(tweet_authors) > 0:
            CHUNK_LEN = 50
            tweet_author_chunks = np.array_split(tweet_authors, (len(tweet_authors)//CHUNK_LEN + 1))
            i = 0
            for chunk in tweet_author_chunks:
                
                # construct filter
                query = f"from:{mention[1:]} is:reply ("
                for c in chunk:
                    query += f'@{c} OR '
                query = query[:-4]
                query += ")"
                
              
                #print(query)
                
                search_results = self.client.search_all(query=query, max_results=100, start_time=startdate, end_time=enddate)
                print(f"request {i+1}/{len(tweet_author_chunks)}...")
                i += 1
                for page in search_results:
                    result = expansions.flatten(page)
                    print(f'Fetching {len(result)} responses...')
                    for tweet in result:
                        
                        # print("\n\n=============================")
                        # print(json.dumps(tweet))
                        # print("=============================\n\n")
                        
                        tweetObj = Tweet(json_str=tweet, corporation=name)
                        self.db_responses.saveTweet(name, tweetObj)
        
                
                
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
       self.db_corp = None
       self.db_responses = None
       self.path = path
        
    def setDB(self, myDB):
        self.db = myDB
        
    def setCorporationDB(self, myDB):
        self.db_corp = myDB
        
    def setResponsesDB(self, myDB):
        self.db_responses = myDB
        
    def writeTweets(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        df = pd.DataFrame(columns=['Company','Account','Date','Cmltv_Mentions','Annual_Mentions','Cmltv_GW_Mentions','Annual_GW_Mentions','Tweet_Text', 'Tweet_Author', 'Author_Handle', 'Retweets_Count', 'Impression', 'Engagement', 'Impression_Estimated',
                                   'Responded_to', 'Responded_to_txt', 'Responded_to_author', 'Responded_to_author_txt'])
        tweets = self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=(corporation, startdate, enddate,))
        
        
        all_mentions = len(self.db.loadTweets("WHERE corporation = ? AND retweeted_tweet_id = ?", args=(corporation,0)))
        
        if self.db_corp is not None:
            all_tweet_counts, tweet_count_total = self.db_corp.loadTweetCounts("WHERE corporation = ?", args=(corporation,), corporation=corporation)
        else:
            tweet_count_total = 0
        
        for tweet in tweets:
            rt = tweet.retweet_count
            if tweet.retweeted_tweet_id > 0:
                rt = -1
                
            eng = tweet.reply_count + tweet.like_count + tweet.quote_count
            if tweet.retweeted_tweet_id == 0:
                eng += tweet.retweet_count
                
                
            dt = tweet.date_posted.replace(tzinfo=None)
            
            
            yearly_mentions = len(self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ? AND retweeted_tweet_id = ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),0)))
            
            
            if self.db_corp is not None:
                yearly_tweet_counts, tweet_count_yearly = self.db_corp.loadTweetCounts("WHERE corporation = ? AND tweet_date > ? AND tweet_date_end < ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)), corporation=corporation)
            else:
                tweet_count_yearly = 0
            
            
            company_responded = False
            company_responded_txt = ""
            at_company_responded = False
            at_company_responded_txt = ""
            if self.db_responses is not None:
                responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_tweet_id = ?", args=(corporation,tweet.tweet_id))
                if len(responses) > 0:
                    company_responded = True
                    company_responded_txt = responses[0].tweet_content
                at_responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_author_id = ?", args=(corporation,tweet.author_id))
                if len(at_responses) > 0:
                    at_company_responded = True
                    at_company_responded_txt = at_responses[0].tweet_content
            
            
            tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Tweet_Author':tweet.author,
                       'Date':dt, 'Author_Handle':tweet.authorhandle, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count,
                       'Impression_Estimated':tweet.follower_count, 'Engagement':eng, 
                       'Cmltv_GW_Mentions':all_mentions, 'Annual_GW_Mentions':yearly_mentions,
                       'Cmltv_Mentions':tweet_count_total, 'Annual_Mentions':tweet_count_yearly,
                       'Responded_to':company_responded, 'Responded_to_txt':company_responded_txt, 
                       'Responded_to_author':at_company_responded, 'Responded_to_author_txt':at_company_responded_txt}
            df = df.append(tweetInfo, ignore_index=True)
        
        df.to_csv(self.path)
        
        
        
        
    def writeUniqueTweets(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        df = pd.DataFrame(columns=['Company','Account','Date','Cmltv_Mentions','Annual_Mentions','Cmltv_GW_Mentions','Annual_GW_Mentions','Tweet_Text', 'Tweet_Author', 'Author_Handle', 'Retweets_Count', 'Impression', 'Engagement', 'Impression_Estimated',
                                   'Responded_to', 'Responded_to_txt', 'Responded_to_author', 'Responded_to_author_txt'])
        tweets = self.db.loadTweets("WHERE retweeted_tweet_id = ? AND corporation = ? AND date_posted > ? AND date_posted < ?", args=(0, corporation, startdate, enddate,))
        
        
        all_mentions = len(self.db.loadTweets("WHERE corporation = ? AND retweeted_tweet_id = ?", args=(corporation,0)))
        
        if self.db_corp is not None:
            all_tweet_counts, tweet_count_total = self.db_corp.loadTweetCounts("WHERE corporation = ?", args=(corporation,), corporation=corporation)
        else:
            tweet_count_total = 0
        
        for tweet in tweets:
            
            tweets_children = self.db.loadTweets("WHERE retweeted_tweet_id = ?", args=(tweet.tweet_id,))
            engagement = tweet.reply_count + tweet.like_count + tweet.quote_count + tweet.retweet_count
            impression = tweet.follower_count
            
            for child in tweets_children:
                #engagement += child.reply_count + child.like_count + child.quote_count
                impression += child.follower_count
            
        
            
            rt = tweet.retweet_count
            dt = tweet.date_posted.replace(tzinfo=None)
            
            yearly_mentions = len(self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ? AND retweeted_tweet_id = ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),0)))
            
            
            if self.db_corp is not None:
                yearly_tweet_counts, tweet_count_yearly = self.db_corp.loadTweetCounts("WHERE corporation = ? AND tweet_date > ? AND tweet_date_end < ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)), corporation=corporation)
            else:
                tweet_count_yearly = 0
                
                
            company_responded = False
            company_responded_txt = ""
            at_company_responded = False
            at_company_responded_txt = ""
            if self.db_responses is not None:
                responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_tweet_id = ?", args=(corporation,tweet.tweet_id))
                if len(responses) > 0:
                    company_responded = True
                    company_responded_txt = responses[0].tweet_content
                at_responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_author_id = ?", args=(corporation,tweet.author_id))
                if len(at_responses) > 0:
                    at_company_responded = True
                    at_company_responded_txt = at_responses[0].tweet_content
            
            tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Tweet_Author':tweet.author,
                       'Date':dt, 'Author_Handle':tweet.authorhandle, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count,
                       'Impression_Estimated':impression, 'Engagement':engagement, 
                       'Cmltv_GW_Mentions':all_mentions, 'Annual_GW_Mentions':yearly_mentions,
                       'Cmltv_Mentions':tweet_count_total, 'Annual_Mentions':tweet_count_yearly,
                       'Responded_to':company_responded, 'Responded_to_txt':company_responded_txt, 
                       'Responded_to_author':at_company_responded, 'Responded_to_author_txt':at_company_responded_txt}
            df = df.append(tweetInfo, ignore_index=True)
        
        df.to_csv(self.path)
        
    def writeAllUniqueTweets(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation_list=[]):
        df = pd.DataFrame(columns=['Company','Account','Date','Cmltv_Mentions','Annual_Mentions','Cmltv_GW_Mentions','Annual_GW_Mentions','Tweet_Text', 'Tweet_Author', 'Author_Handle', 'Retweets_Count', 'Impression', 'Engagement', 'Impression_Estimated',
                                   'Responded_to', 'Responded_to_txt', 'Responded_to_author', 'Responded_to_author_txt'])
        
        for corporation in corporation_list:
            tweets = self.db.loadTweets("WHERE retweeted_tweet_id = ? AND corporation = ? AND date_posted > ? AND date_posted < ?", args=(0, corporation, startdate, enddate,))
            
            
            all_mentions = len(self.db.loadTweets("WHERE corporation = ? AND retweeted_tweet_id = ?", args=(corporation,0)))
            
            if self.db_corp is not None:
                all_tweet_counts, tweet_count_total = self.db_corp.loadTweetCounts("WHERE corporation = ?", args=(corporation,), corporation=corporation)
            else:
                tweet_count_total = 0
            
            for tweet in tweets:
                
                tweets_children = self.db.loadTweets("WHERE retweeted_tweet_id = ?", args=(tweet.tweet_id,))
                engagement = tweet.reply_count + tweet.like_count + tweet.quote_count + tweet.retweet_count
                impression = tweet.follower_count
                
                for child in tweets_children:
                    #engagement += child.reply_count + child.like_count + child.quote_count
                    impression += child.follower_count
                
            
                
                rt = tweet.retweet_count
                dt = tweet.date_posted.replace(tzinfo=None)
                
                yearly_mentions = len(self.db.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ? AND retweeted_tweet_id = ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),0)))
                
                
                if self.db_corp is not None:
                    yearly_tweet_counts, tweet_count_yearly = self.db_corp.loadTweetCounts("WHERE corporation = ? AND tweet_date > ? AND tweet_date_end < ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)), corporation=corporation)
                else:
                    tweet_count_yearly = 0
                    
                company_responded = False
                company_responded_txt = ""
                at_company_responded = False
                at_company_responded_txt = ""
                if self.db_responses is not None:
                    responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_tweet_id = ?", args=(corporation,tweet.tweet_id))
                    if len(responses) > 0:
                        company_responded = True
                        company_responded_txt = responses[0].tweet_content
                    at_responses = self.db_responses.loadTweets("WHERE corporation = ? AND responded_author_id = ?", args=(corporation,tweet.author_id))
                    if len(at_responses) > 0:
                        at_company_responded = True
                        at_company_responded_txt = at_responses[0].tweet_content
                
                tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Tweet_Author':tweet.author,
                           'Date':dt, 'Author_Handle':tweet.authorhandle, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count,
                           'Impression_Estimated':impression, 'Engagement':engagement, 
                           'Cmltv_GW_Mentions':all_mentions, 'Annual_GW_Mentions':yearly_mentions,
                           'Cmltv_Mentions':tweet_count_total, 'Annual_Mentions':tweet_count_yearly,
                           'Responded_to':company_responded, 'Responded_to_txt':company_responded_txt, 
                           'Responded_to_author':at_company_responded, 'Responded_to_author_txt':at_company_responded_txt}
                df = df.append(tweetInfo, ignore_index=True)
        
        df.to_csv(self.path)
        
    def writeResponses(self, startdate=config.START_DATE, enddate=datetime.datetime.now(datetime.timezone.utc), corporation=""):
        df = pd.DataFrame(columns=['Company','Account','GW_Response','Date','Cmltv_Responses','Annual_Responses','Tweet_Text', 'Replied_To_Author', 'Replied_To', 'Retweets_Count', 'Impression', 'Engagement', 'Impression_Estimated'])
        tweets = self.db_responses.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=(corporation, startdate, enddate,))
        
        
        all_responses = len(self.db_responses.loadTweets("WHERE corporation = ?", args=(corporation,)))
        
       
        for tweet in tweets:
            rt = tweet.retweet_count
            if tweet.retweeted_tweet_id > 0:
                rt = -1
                
            eng = tweet.reply_count + tweet.like_count + tweet.quote_count
            if tweet.retweeted_tweet_id == 0:
                eng += tweet.retweet_count
                
                
            dt = tweet.date_posted.replace(tzinfo=None)
            
            
            yearly_responses = len(self.db_responses.loadTweets("WHERE corporation = ? AND date_posted > ? AND date_posted < ?", args=(corporation,datetime.datetime(dt.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc),datetime.datetime(dt.year + 1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc))))
            
            
            tweet_og = self.db_responses.loadTweets("WHERE tweet_id = ?", args=(tweet.responded_tweet_id,))
            
            gw_response = False
            if len(tweet_og) > 0:
                gw_response = True
        
            
            
            tweetInfo = {'Company':corporation, 'Account':config.TWITTER_NAMES[corporation], 'Replied_To':tweet.responded_tweet_id,
                       'Date':dt, 'Tweet_Text':tweet.tweet_content, 'Retweets_Count':rt, 'Impression':tweet.impression_count,
                       'Impression_Estimated':tweet.follower_count, 'Engagement':eng, 
                       'Cmltv_Responses':all_responses, 'Annual_Responses':yearly_responses,
                       'GW_Response':gw_response,'Replied_To_Author':tweet.responded_author_id}
            df = df.append(tweetInfo, ignore_index=True)
        
        df.to_csv(self.path)
     
        
    

        