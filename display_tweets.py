# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 12:06:58 2023

@author: felix
"""

from greenwashing import GreenwashingDB, GreenwashingPlots, GreenwashingExcel, CorporationDB, GreenwashingResponsesDB
import os
import datetime
import config


if __name__ == '__main__':
    
    # connect to sql database
    myDB = GreenwashingDB(os.path.join(config.PATH_DATA, config.DB_NAME))
    myDB.setTable(config.DB_TABLE_NAME)
    
    # if you've done this step, otherwise remove this
    myDB2 = CorporationDB(os.path.join(config.PATH_DATA, config.DB_NAME2))
    myDB2.setTable(config.DB_TABLE_NAME2)
    
    
    myDBresponse = GreenwashingResponsesDB(os.path.join(config.PATH_DATA, config.DB_NAME3))
    myDBresponse.createTable(config.DB_TABLE_NAME3)
    
    for corporation in config.CORPORATION_LIST:
        
        print(f"Creating files for {corporation}...")
        

        gwPlots = GreenwashingPlots()
        gwPlots.setDB(myDB)
        
        
        gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, f"{corporation}_all.csv"))
        gwExcel.setDB(myDB)
        gwExcel.setResponsesDB(myDBresponse)
        gwExcel.setCorporationDB(myDB2)
        
       
        gwExcel2 = GreenwashingExcel(os.path.join(config.PATH_DATA, f"{corporation}_unique.csv"))
        gwExcel2.setDB(myDB)
        gwExcel2.setResponsesDB(myDBresponse)
        gwExcel2.setCorporationDB(myDB2)
        
        
        
        
        
        
        # plotting options
        
        # # plot a number of tweets over time
        # # gwPlots.plotViewsVSTime(startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), corporation="yumbrands")
        
        
        # pull a number of tweets into an excel file
        print("All Tweets...")
        gwExcel.writeTweets(corporation=corporation)
        
        # pull unique tweets into an excel file
        print("Unique Tweets...")
        gwExcel2.writeUniqueTweets(corporation=corporation)
        
    print(f"Creating summary file")
        
    gwExcel3 = GreenwashingExcel(os.path.join(config.PATH_DATA, r"all_tweets.csv"))
    gwExcel3.setDB(myDB)
    gwExcel3.setResponsesDB(myDBresponse)
    gwExcel3.setCorporationDB(myDB2)
        
    gwExcel3.writeAllUniqueTweets(corporation_list = config.CORPORATION_LIST)
        

    
    
    myDB.close()
    myDB2.close()
    myDBresponse.close()