# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 12:06:58 2023

@author: felix
"""

from greenwashing import GreenwashingDB, GreenwashingPlots, GreenwashingExcel, CorporationDB
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
    
    
    gwPlots = GreenwashingPlots()
    gwPlots.setDB(myDB)
    
    gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, r"amazon_all.xlsx"))
    gwExcel.setDB(myDB)
    gwExcel.setCorporationDB(myDB2)
    
    gwExcel2 = GreenwashingExcel(os.path.join(config.PATH_DATA, r"amazon_unique.xlsx"))
    gwExcel2.setDB(myDB)
    gwExcel2.setCorporationDB(myDB2)
    
    
    
    
    # plotting options
    
    # # plot a number of tweets over time
    # # gwPlots.plotViewsVSTime(startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), corporation="yumbrands")
    
    
    # pull a number of tweets into an excel file
    gwExcel.writeTweets(corporation="amazon")
    
    # pull unique tweets into an excel file
    gwExcel2.writeUniqueTweets(corporation="amazon")
        

    
    
    myDB.close()
    myDB2.close()