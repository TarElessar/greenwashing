# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 11:59:52 2023

@author: felix
"""


from greenwashing import GreenwashingDB, GreenwashingMainObj
import os
import config





if __name__ == '__main__':
    
    # connect to sql database
    myDB = GreenwashingDB(os.path.join(config.PATH_DATA, config.DB_NAME))
    myDB.createTable(config.DB_TABLE_NAME)

    
    #set up filters, etc
    myGreenwashing = GreenwashingMainObj()
    myGreenwashing.setDB(myDB)
    myGreenwashing.setBearerToken(config.BEARER_TOKEN)
    
    
    # corporations
    for corporation in config.CORPORATION_LIST:
        myGreenwashing.fetchTweets(tweetfilter=config.GREENWASH_FILTER, name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE)
    
    
    #myGreenwashing.printSingleTweets(['1502324722037624846'])
    
    # myDB.listTable()
    myDB.close()
    