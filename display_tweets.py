# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 12:06:58 2023

@author: felix
"""

from greenwashing import GreenwashingDB, GreenwashingPlots, GreenwashingExcel
import os
import datetime
import config


if __name__ == '__main__':
    
    # connect to sql database
    myDB = GreenwashingDB(os.path.join(config.PATH_DATA, config.DB_NAME))
    myDB.setTable(config.DB_TABLE_NAME)
    
    
    gwPlots = GreenwashingPlots()
    gwPlots.setDB(myDB)
    
    gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, r"yumbrands_all.xlsx"))
    gwExcel.setDB(myDB)
    
    # plot a number of tweets over time
    # gwPlots.plotViewsVSTime(startdate=datetime.datetime(2020, 10, 1, 0, 0, 0, 0, datetime.timezone.utc), corporation="yumbrands")
    
    
    # pull a number of tweets into an excel file
    # gwExcel.writeTweets(corporation="yumbrands")
    
    # pull unique tweets into an excel file
    gwExcel.writeUniqueTweets(corporation="yumbrands")
        

    
    
    myDB.close()