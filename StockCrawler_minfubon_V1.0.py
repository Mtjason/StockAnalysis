from fubon_neo.sdk import FubonSDK, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction
import twstock
import requests as r
import pandas as pd
from bs4 import BeautifulSoup
import datetime
import pytz
import json
import pymysql
#import mysql.connector
import yfinance
from io import StringIO
import numpy as np
import importlib
import numpy as np
from time import sleep
from function import functions
from function import MySQLfunctions
import tkinter as tk
from tkinter import messagebox
from sqlalchemy import create_engine
import math

sdk = FubonSDK()

accounts = sdk.login("A130216973", "Tang8264", r"C:\CAFubon\A130216973\A130216973.pfx", "JT205122")

#------------------------------Confidential------------------------------
'''
Result {
  is_success: False,
  message: 無簽署完成API使用風險暨聲明書帳號，請與營業員聯絡,
  data: None
}
'''
# 資料庫參數設定
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "jason8744",
    "database": "twstockinfo"
}

connection = pymysql.connect(**db_settings,
                             connect_timeout=60) # 設置超時時間
# 建立游標物件
cursor = connection.cursor()
query = """ SELECT * FROM twstockinfo.twstocktable 
WHERE datetime BETWEEN '2024-11-18 00:00:00' AND '2024-12-13 23:59:59';"""
df = pd.read_sql(query, connection) # type -> pandas.core.frame.DataFrame


#stockIDs = df['id'].unique()
stockIDs = df[['id', 'name']].drop_duplicates()
df = df.set_index(['id', 'datetime'], drop=False)

sdk.init_realtime() # 建立行情連線
reststock = sdk.marketdata.rest_client.stock

insert_query = "INSERT IGNORE INTO twstockinfo.twstockfubon1m (id, name, datetime, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s, %s)"
data_tuples = []

for i in range(228,len(stockIDs)) :
    print("Processing : " + stockIDs['id'].iloc[i])
    fubonData = reststock.historical.candles(**{"symbol": stockIDs['id'].iloc[i], "from": "2023-11-18", "to": "2023-12-13", "timeframe" : "1"})
    if len(fubonData) == 2 : #抓到空的
        continue
    #cursor = connection.cursor()
    
    minInfo = fubonData['data']
    
    for item in minInfo:
        dt_with_tz= datetime.datetime.fromisoformat(item['date'])
        date = dt_with_tz.replace(tzinfo=None)
        data_tuple = (
            stockIDs['id'].iloc[i],
            stockIDs['name'].iloc[i],
            date,
            item['open'],
            item['high'],
            item['low'],
            item['close']
        )
        data_tuples.append(data_tuple)
    
    cursor.executemany(insert_query, data_tuples)
    connection.commit()

# 關閉連接
connection.close()
#a['data'][0]['date']