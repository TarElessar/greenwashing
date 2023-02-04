# -*- coding: utf-8 -*-
"""
Created on Sat Feb  4 10:55:33 2023

@author: felix
"""



from greenwashing import GreenwashingDB, GreenwashingMainObj, CorporationDB, CorporationInfoMain, GreenwashingExcel, GreenwashingResponsesDB
import os
import config





if __name__ == '__main__':
    
    # connect to sql database
    myDB = GreenwashingDB(os.path.join(config.PATH_DATA, config.DB_NAME))
    myDB.createTable(config.DB_TABLE_NAME)
    
    myDBresponse = GreenwashingResponsesDB(os.path.join(config.PATH_DATA, config.DB_NAME3))
    myDBresponse.createTable(config.DB_TABLE_NAME3)

    
    #set up filters, etc
    myGreenwashing = GreenwashingMainObj()
    myGreenwashing.setDB(myDB)
    myGreenwashing.setDBresponses(myDBresponse)
    myGreenwashing.setBearerToken(config.BEARER_TOKEN)
    
    
    # searchResponses will search replies to the GW tweets
    # searchResponsesFast will search replies to GW tweet authors and let you know if the parent tweet was a GW tweet
    # if you want both, you should probably put them into a separate database
    for corporation in config.CORPORATION_LIST:
        #myGreenwashing.searchResponses(name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE, enddate=config.END_DATE)
        myGreenwashing.searchResponsesFast(name=corporation, mention=config.TWITTER_NAMES[corporation], startdate=config.START_DATE, enddate=config.END_DATE)
        
        pass
        
      
    for corporation in config.CORPORATION_LIST:
           
        print(f"Creating response files for {corporation}...")
        
        gwExcel = GreenwashingExcel(os.path.join(config.PATH_DATA, f"{corporation}_responses.csv"))
        gwExcel.setDB(myDB)
        gwExcel.setResponsesDB(myDBresponse)
        

        gwExcel.writeResponses(corporation=corporation)
        
        
        
    
    #myDB.listTable()
    myDB.close()
    
    #myDBresponse.listTable()
    myDBresponse.close()