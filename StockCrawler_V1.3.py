import twstock
import requests as r
import pandas as pd
from bs4 import BeautifulSoup
import datetime
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
import sys
#%reset
#latest record 11/14


#No loop
#stock_openday = functions.stock_days(2024, 1) #list-twstock.stock.Data
keepGoing = functions.stock_days(2023, 1)
stock_openday =  keepGoing[6]

#確認是List(為了單一日期插入)
if isinstance(stock_openday, list):
    pass
else:
    stock_openday = [stock_openday]
 

url_goodinfo = 'https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E4%B8%8A%E5%B8%82&INDUSTRY_CAT=%E4%B8%8A%E5%B8%82%E5%85%A8%E9%83%A8&SHEET=%E5%85%AC%E5%8F%B8%E5%9F%BA%E6%9C%AC%E8%B3%87%E6%96%99'
goodinfoStockTable = functions.webcrawler_goodinfo_stocktable(url_goodinfo) #For 發行張數
#goodinfoStockTableID = goodinfoStockTable['代號'] #確認沒有亂序
url_goodinfo_OTC = 'https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E4%B8%8A%E6%AB%83&INDUSTRY_CAT=%E4%B8%8A%E6%AB%83%E5%85%A8%E9%83%A8&SHEET=%E5%85%AC%E5%8F%B8%E5%9F%BA%E6%9C%AC%E8%B3%87%E6%96%99'
goodinfoStockTable_OTC = functions.webcrawler_goodinfo_stocktable(url_goodinfo_OTC) 

delay = 0.1

# 資料庫參數設定
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "twstockinfo"
}

try:
    # 建立資料庫連接
    connection = pymysql.connect(**db_settings,
                                 connect_timeout=60) # 設置超時時間
    # 建立游標物件
    cursor = connection.cursor()
    
    
    #清空資料庫 *Warning
    #truncate_table_query = "TRUNCATE TABLE twstocktable"
    #cursor.execute(truncate_table_query)
    #connection.commit()
    
    # 清除指定日期 *****
    ''' #被刪除了，最後記得要補....
    clear_datetime_query = """
    DELETE FROM twstocktable 
    WHERE datetime >= '2023-01-11 00:00:00' AND datetime <= '2023-01-11 23:59:59'
    """
    cursor.execute(clear_datetime_query)
    connection.commit()
    '''
    
    
    # 插入資料
    sql = """INSERT IGNORE INTO twstocktable (id, name, datetime, capacity, turnover, open, high,
        low, close, bigChipForeign, bigChipInvest, bigChipDealer, yield, PE, totalShare, netRatio) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    #functions.stocktable_goodinfo_get('2330',goodinfoStockTable,'發行量  (萬張)')
    day = 0
    for index in range(len(stock_openday)): #date loop
        i = stock_openday[index]
        print('Processing ' + str(i.date))
        #TWSE main stock table
        date2twse = functions.date_font(i.date, 'twse') 
        url_twse = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date='+date2twse+'&type=ALLBUT0999'
        date2Chinese = functions.date_font(i.date,'chinese')
        twseStockTable = functions.webcrawler_twse_main_stocktable(url_twse,date2Chinese," 每日收盤行情(全部(不含權證、牛熊證))") 
        twseStockTableData = twseStockTable['data'] #list(stockID)-list
        
            #TWSE chip table
        url_twse_chip = 'https://www.twse.com.tw/rwd/zh/fund/T86?date='+date2twse+'&selectType=ALLBUT0999'
        twseStockChipTable = functions.webcrawler_stocktable(url_twse_chip)
        twseStockChipTableData = twseStockChipTable['data'] 
        
            #TWSE info table
        url_twse_info = 'https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date='+date2twse+'&selectType=ALL'
        twseStockInfoTable = functions.webcrawler_stocktable(url_twse_info)
        twseStockInfoTableData = twseStockInfoTable['data']       
        
        for j in twseStockTableData:
            #main_info = twseStockTableData[j]
            stock_ID = j[0]
            name = j[1]
            capacity = functions.convert_to_num(j[2]) #成交股數
            turnover = functions.convert_to_num(j[4]) #成交量
            Open = functions.convert_to_num(j[5])
            high = functions.convert_to_num(j[6])
            low = functions.convert_to_num(j[7])
            close = functions.convert_to_num(j[8])
            bigChipForeign = functions.search_array(twseStockChipTableData, stock_ID, 4) + functions.search_array(twseStockChipTableData, stock_ID, 7) #外陸資+外資
            bigChipInvest = functions.search_array(twseStockChipTableData, stock_ID, 10)
            bigChipDealer = functions.search_array(twseStockChipTableData, stock_ID, 11)
            Yield = functions.search_array(twseStockInfoTableData, stock_ID, 3)
            PE = functions.convert_to_num(j[15])
            totalShare = functions.stocktable_goodinfo_get(stock_ID,goodinfoStockTable,'發行量  (萬張)')
            netRatio = functions.search_array(twseStockInfoTableData, stock_ID, 6)
            
            cursor.execute(sql, (
                stock_ID,
                name,
                stock_openday[day].date,  # 假設使用第一個元素的日期
                capacity,
                turnover,
                Open,
                high,
                low,
                close,
                bigChipForeign, #股數
                bigChipInvest,
                bigChipDealer,
                Yield,
                PE,
                totalShare,
                netRatio
            ))
            connection.commit()
            sleep(delay)
                    
        
        #TPE main stock table
        date2tpex = functions.date_font(i.date, 'tpex') 

        payload_tpex = {
            "date": date2tpex,
            "type": "EW", #這行超必要
            "response": "json"
        }
        
        headers_tpex = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,eo;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Host": "www.tpex.org.tw",
            "Origin": "https://www.tpex.org.tw",
            "Sec-CH-UA": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
            }
        
        url_tpex = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/otc'
        tpexStockTable = functions.webcrawler_stocktable_post(url_tpex, headers_tpex, payload_tpex)
        tpexStockTableData = tpexStockTable['tables'][0]['data']
        #tpexStockTableData = tpexStockTable['aaData']
        
            #TPEX chip table
        payload_tpex_chip = {
            "date": date2tpex,
            "type": "Daily",
            "response": "json"
        }            
        url_tpex_chip = 'https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade'
        tpexStockChipTable = functions.webcrawler_stocktable_post(url_tpex_chip , headers_tpex, payload_tpex_chip)
        tpexStockChipTableData = tpexStockChipTable['tables'][0]['data']
        #tpexStockChipTableData = tpexStockChipTable['aaData']
        
        
            #TPEX info table
        payload_tpex_info = {
            "date": date2tpex,
            "response": "json"
        }
        url_tpex_info = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate'
        tpexStockInfoTable = functions.webcrawler_stocktable_post(url_tpex_info , headers_tpex, payload_tpex_info)
        tpexStockInfoTableData = tpexStockInfoTable['tables'][0]['data']
        #tpexStockInfoTableData = tpexStockInfoTable['aaData']
        
        for k in tpexStockTableData:
            #main_info = twseStockTableData[j]
            stock_ID = k[0]
            name = k[1]
            capacity = functions.convert_to_num(k[7]) #成交股數
            turnover = functions.convert_to_num(k[8]) #成交量
            Open = functions.convert_to_num(k[4])
            high = functions.convert_to_num(k[5])
            low = functions.convert_to_num(k[6])
            close = functions.convert_to_num(k[2])
            bigChipForeign = functions.search_array(tpexStockChipTableData, stock_ID, 10) #外陸資+外資
            bigChipInvest = functions.search_array(tpexStockChipTableData, stock_ID, 13)
            bigChipDealer = functions.search_array(tpexStockChipTableData, stock_ID, 22)
            Yield = functions.search_array(tpexStockInfoTableData, stock_ID, 5)
            PE = functions.search_array(tpexStockInfoTableData, stock_ID, 2)
            totalShare = functions.stocktable_goodinfo_get(stock_ID,goodinfoStockTable_OTC,'發行量  (萬張)')
            netRatio = functions.search_array(tpexStockInfoTableData, stock_ID, 6)
            
            cursor.execute(sql, (
                stock_ID,
                name,
                stock_openday[day].date,  # 假設使用第一個元素的日期
                capacity,
                turnover,
                Open,
                high,
                low,
                close,
                bigChipForeign,
                bigChipInvest,
                bigChipDealer,
                Yield,
                PE,
                totalShare,
                netRatio
            ))
            connection.commit()
            sleep(delay)
            
        day +=1
except pymysql.MySQLError as e:
    print("Error: unable to connect to the database")
    print(e)

finally:
    # 關閉連接
    if connection:
        connection.close()



'''        
url_twse = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20241025&type=ALLBUT0999'
dateRaw = datetime.datetime(2024, 10, 25, 0, 0)
date2Chinese = functions.date_font(dateRaw,'chinese')
twseStockTable = functions.webcrawler_twse_main_stocktable(url_twse,date2Chinese) 
date2tpe = functions.date_font(dateRaw,'tpe')

twseStockTableData = twseStockTable['data'] #list-list
twseStockID = []
for i in range(len(twseStockTableData)):
    twseStockID.append(twseStockTableData[i][0])
    
'''
