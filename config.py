# -*- coding: utf-8 -*-
"""
Created on Sun Jan 22 20:49:19 2023

@author: felix
"""

import datetime


# you need to change these
PATH_DATA = r"D:\Documents\leting\data"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAFiBlQEAAAAASLTArRQ9YipSoM810aURkPUzUWo%3DkXZqFHrCVAIND4uI34bhSxMUlM8zQ3TkeVq1uxDqGDwr9Acg1T"


# you may want to change these
DB_NAME = "greenwashing_main.db"
DB_NAME2 = "corporation_info_main.db"
DB_TABLE_NAME = "gw1"
DB_TABLE_NAME2 = "gw2"


# parameters below are for your filters, etc - the corporation names should have no spaces, e.g. use yumbrands
GREENWASH_FILTER = "greenwash OR greenwashing OR #greenwashing OR #greenwash OR #sustainabilitywashing OR #fakestewardship OR #greenwashingalert OR #greenwashingexposed OR #fooledbymarketing OR #greenwashingfraud OR #notreallygreen OR #greenwashingscandal"
CORPORATION_LIST = ["amazon"] 
START_DATE = datetime.datetime(2022, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)
END_DATE = datetime.datetime(2023, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)

TWITTER_NAMES = {"amazon" : "@amazon"}