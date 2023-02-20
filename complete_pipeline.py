# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 11:59:52 2023

@author: felix
"""


from greenwashing import GreenwashingDB, GreenwashingMainObj, CorporationDB, CorporationInfoMain, GreenwashingResponsesDB, GreenwashingExcel, CorporationInfoMain
import os
import config





if __name__ == '__main__':
    
    # connect to sql database
    myDB = GreenwashingDB(os.path.join(config.PATH_DATA, config.DB_NAME))
    myDB.createTable(config.DB_TABLE_NAME)
    
    myDB2 = CorporationDB(os.path.join(config.PATH_DATA, config.DB_NAME2))
    myDB2.createTable(config.DB_TABLE_NAME2)
    
    myDBresponse = GreenwashingResponsesDB(os.path.join(config.PATH_DATA, config.DB_NAME3))
    myDBresponse.createTable(config.DB_TABLE_NAME3)

    
    #set up filters, etc
    myGreenwashing = GreenwashingMainObj()
    myGreenwashing.setDB(myDB)
    myGreenwashing.setDBresponses(myDBresponse)
    myGreenwashing.setBearerToken(config.BEARER_TOKEN)
    

    myCorporation = CorporationInfoMain()
    myCorporation.setDB(myDB2)
    myCorporation.setBearerToken(config.BEARER_TOKEN)
    
    cnt = 1

    for corporation in config.CORPORATION_LIST:
        
        print(f"\n====================================\n{cnt}/{len(config.CORPORATION_LIST)}\n====================================\n")
        cnt += 1
        
        print(f"Fetching tweets for {corporation}")
        myGreenwashing.fetchTweets(tweetfilter=config.GREENWASH_FILTER, name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE, enddate=config.END_DATE, include_replies=False)
        print(f"Fetching counts for {corporation}")
        myCorporation.fetchTweets(name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE, enddate=config.END_DATE)
    
        print(f"Writing tweets for {corporation}")
        gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, f"all_tweets_{corporation}.csv"))
        gwExcel.setDB(myDB)
        gwExcel.setResponsesDB(myDBresponse)
        gwExcel.setCorporationDB(myDB2)
        gwExcel.writeTweets(corporation=corporation)
        
        print(f"Writing unique tweets for {corporation}")
        gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, f"unique_tweets_{corporation}.csv"))
        gwExcel.setDB(myDB)
        gwExcel.setResponsesDB(myDBresponse)
        gwExcel.setCorporationDB(myDB2)
        gwExcel.writeUniqueTweets(corporation=corporation)
    
        print(f"Fetching responses for {corporation}")
        myGreenwashing.searchResponsesFast(name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE, enddate=config.END_DATE)
        
        print(f"Writing responses for {corporation}...")
        gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, f"responses_{corporation}.csv"))
        gwExcel.setDB(myDB)
        gwExcel.setResponsesDB(myDBresponse)
        gwExcel.writeResponses(corporation=corporation)
        
    print(f"Creating summary file")
    gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, r"tweets_unique_summary.csv"))
    gwExcel.setDB(myDB)
    gwExcel.setResponsesDB(myDBresponse)
    gwExcel.setCorporationDB(myDB2)
    gwExcel.writeAllUniqueTweets(corporation_list = config.CORPORATION_LIST)
    
    myDB.close()
    myDB2.close()
    myDBresponse.close()