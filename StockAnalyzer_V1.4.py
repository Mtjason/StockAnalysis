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
from sqlalchemy import create_engine
import math


'''
# 創建主窗口
root = tk.Tk()
root.withdraw()  # 隱藏主窗口
# 調用確認函數
functions.on_run()
# 關閉主窗口
root.destroy()
'''


def checkPriceExist(df,priceType,index) : #避免nan導致計算偏差
    indexC = index
    while pd.isna(df[priceType].loc[indexC]) :
        indexC = indexC + 1
    return df[priceType].loc[indexC]


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
query = """ SELECT * FROM twstocktable 
WHERE datetime BETWEEN '2023-07-10 00:00:00' AND '2024-12-31 23:59:59';"""
df = pd.read_sql(query, connection) # type -> pandas.core.frame.DataFrame


# 關閉連接
connection.close()
stockIDs = df['id'].unique()
df = df.set_index(['id', 'datetime'], drop=False)

#找到連續買賣超日>3，且累計大於發行量1%的股票，並標上達標法人及連續天數


businessDays = functions.stock_days(2023, 1)
df_chip = pd.DataFrame([])
df_gain = pd.DataFrame([])
df_strategy1_detected = pd.DataFrame([])
rewinddays = 120
drop = 0.93

for ID in stockIDs :
    if ID[0:2] == '00': #排除基金
        continue
    print('StockAnalyzer_V1.4 Processing stockID : '+ID)
    dfID = df.loc[ID,slice(None)]
    #dfID =  df[df['id'] == ID] #日期從小到大
    dfID.reset_index(drop=True, inplace=True)
    dfID = dfID.sort_values(by='datetime', ascending=True).reset_index(drop=True) #目前日期排列小到大
    
    dayInspected = len(dfID) - 30 - rewinddays #25=回測天數是用來回測
    if dayInspected < 0 :
        continue
    consec = 0
    for day in range(dayInspected):
        
        a = 0
        b = 0
        
        #120營業日, 股票上漲未超過30%
        startIndex = day+rewinddays
        checkDay = dfID.loc[startIndex] #+119
        minCloseLimit = min(dfID['close'].loc[day:startIndex])*1.3 #pandas.core.series.Series [0:119]
        if checkDay['close'] > minCloseLimit : #如果120營業日, 股票上漲30%則跳出這輪
            continue
        
        #財報面健檢
        if checkDay['PE'] > 20 or checkDay['netRatio'] > 3.5 or checkDay['yield'] < 2.5 :
            continue

        #Scenerio1 新版: 法人30營業累計買賣累計超0.8%
        if checkDay['totalShare'] == 0 :
            continue
        checkDay30 =  dfID.loc[startIndex-29:startIndex]
        checkDay30.reset_index(drop=True, inplace=True) #目前日期排列小到大
        accumChipForeign30 = (checkDay30['bigChipForeign'].sum())/(checkDay['totalShare']*10**7)
        accumChipInvest30 = (checkDay30['bigChipInvest'].sum())/(checkDay['totalShare']*10**7)
        accumChipDealer30 = (checkDay30['bigChipDealer'].sum())/(checkDay['totalShare']*10**7)
        if accumChipForeign30 >=0.008 or accumChipInvest30 >=0.008 or accumChipDealer30 >=0.008 :
            df_chip_new = pd.DataFrame({
                "id" : [checkDay['id']],
                "name" : [checkDay['name']],
                "datetime" : [checkDay['datetime']],
                "open" : [checkDay['open']],
                'accumChipForeign30' : [accumChipForeign30],
                'accumChipInvest30' : [accumChipInvest30],
                'accumChipDealer30' : [accumChipDealer30],
                })
            df_chip = pd.concat([df_chip, df_chip_new], ignore_index=True)
            a = 1


        #Scenerio 2 近20營業日漲停又回落--> 找近30天股價單日突破9%or兩天15%or三日突破20%
        
        for i in range(len(checkDay30)) :
            gain1day = (checkDay30.loc[i]['high'])/(checkDay30.loc[i]['open'])-1
            if gain1day > 0.09 :
                df_gain_new = pd.DataFrame({
                    "id" : [checkDay['id']],
                    "name" : [checkDay['name']],
                    "datetime" : [checkDay['datetime']],
                    "type" : ["Gain 9% in 1 day"],                                  
                    })
                df_gain = pd.concat([df_gain, df_gain_new])
                b = 1
                break
                
            if i < 29 : #避免OOR
                gain2day = (checkDay30.loc[i+1]['high'])/(checkDay30.loc[i]['open'])-1
                if gain2day > 0.15 :
                    df_gain_new = pd.DataFrame({
                        "id" : [checkDay['id']],
                        "name" : [checkDay['name']],
                        "datetime" : [checkDay['datetime']],
                        "type" : ["Gain 15% in 2 day"],                                  
                        })
                    df_gain = pd.concat([df_gain, df_gain_new])
                    b = 1
                    break
                        
                    
            if i < 28 : #避免OOR
                gain3day = (checkDay30.loc[i+2]['high'])/(checkDay30.loc[i]['open'])-1
                if gain3day > 0.2 :
                    df_gain_new = pd.DataFrame({
                        "id" : [checkDay['id']],
                        "name" : [checkDay['name']],
                        "datetime" : [checkDay['datetime']],
                        "type" : ["Gain 20% in 3 day"],                                  
                        })
                    df_gain = pd.concat([df_gain, df_gain_new])
                    b = 1
                    break
        
        
        
        if a and b and consec==0 :
            buyInCheck = dfID.loc[startIndex+1:]
            forwardDays = len(buyInCheck)
            for daypass in range(forwardDays):
                if buyInCheck.loc[startIndex+1+daypass]['low'] <= checkDay['close']*drop and (forwardDays-daypass)>=5:
                    buyInDayIndex = startIndex+1+daypass
                    indexLimit = len(dfID)
                    
                    
                    #當下趨勢分析
                    buyInHigh = buyInCheck.loc[buyInDayIndex]['high']
                    slope = 0
                    capacity2Share = 0
                    for j in range(1,31) :
                        #趨勢
                        highPoint = dfID.loc[buyInDayIndex-j]['high']
                        slope_new = (buyInHigh-highPoint)/j
                        slope = slope_new if slope > slope_new else slope
                        #成交股本比
                        capacity2Share_new =  dfID.loc[buyInDayIndex-j]['capacity']/(dfID.loc[buyInDayIndex-j]['totalShare']*(10**7))
                        capacity2Share = capacity2Share_new if capacity2Share < capacity2Share_new else capacity2Share
                        
                    #buyIn = buyInCheck.loc[buyInDayIndex]['low']
                    buyInDay = buyInCheck.loc[buyInDayIndex]['datetime']
                    buyIn = checkDay['close']*drop
                    
                    close = dfID['close']
                    #Open = dfID['open']
                    #buyIn = Open.loc[startIndex+1]
                    
                    buyInAccumChip30 = dfID.loc[buyInDayIndex-29:buyInDayIndex]
                    AFC30 = buyInAccumChip30['bigChipForeign'].sum()/(buyInCheck.loc[buyInDayIndex]['totalShare']*10**7) #買入時的法人累積
                    ACI30 = buyInAccumChip30['bigChipInvest'].sum()/(buyInCheck.loc[buyInDayIndex]['totalShare']*10**7)
                    ACD30 = buyInAccumChip30['bigChipDealer'].sum()/(buyInCheck.loc[buyInDayIndex]['totalShare']*10**7)
                    
                    
                    df_strategy1_detected_new = pd.DataFrame({
                        "id" : [checkDay['id']],
                        "name" : [checkDay['name']],
                        "datetime" : [checkDay['datetime']],
                        "close" : [checkDay['close']],
                        'buyInDay' : [buyInDay],
                        'buyIn' : [buyIn],
                        'gainfirstday' : [(buyInCheck.loc[buyInDayIndex]['close']-buyIn)*100/buyIn],
                        'gainafter5day' : [(close.loc[buyInDayIndex + 5]-buyIn)*100/buyIn] if buyInDayIndex + 5 < len(dfID)-1 else None,
                        'gainafter10day' : [(close.loc[buyInDayIndex + 10]-buyIn)*100/buyIn] if buyInDayIndex + 10 < len(dfID)-1 else None,
                        'gainafter15day' : [(close.loc[buyInDayIndex + 15]-buyIn)*100/buyIn] if buyInDayIndex + 15 < len(dfID)-1 else None,
                        'gainafter20day' : [(close.loc[buyInDayIndex + 20]-buyIn)*100/buyIn] if buyInDayIndex + 20 < len(dfID)-1 else None,
                        'gainafter25day' : [(close.loc[buyInDayIndex + 25]-buyIn)*100/buyIn] if buyInDayIndex + 25 < len(dfID)-1 else None,
                    
                        'gainafter30day' : [(close.loc[buyInDayIndex + 30]-buyIn)*100/buyIn] if buyInDayIndex + 30 < len(dfID)-1 else None,
                        #'gainafter35day' : [(close.loc[startIndex + 36]-buyIn)/buyIn],
                        #'gainafter40day' : [(close.loc[startIndex + 41]-buyIn)/buyIn],
                        #'gainafter45day' : [(close.loc[startIndex + 46]-buyIn)/buyIn],
                        'accumChipForeign30' : [AFC30], # [accumChipForeign30],
                        'accumChipInvest30' : [ACI30], #[accumChipInvest30],
                        'accumChipDealer30' : [ACD30], #[accumChipDealer30],
                        'trend' : [slope],
                        'highestCapacity30' : [capacity2Share],
                      
                        #"type" : [df_gain_new['type']],
                
                        })   
                    df_strategy1_detected = pd.concat([df_strategy1_detected, df_strategy1_detected_new], ignore_index=True)
                    
                    #consec = 0
                    consec = 1
                    break
       
        if consec :
            consec = 0
            

df_strategy1_detected = df_strategy1_detected.drop_duplicates(subset=["id", "buyInDay"], keep="last")
resultMean = pd.DataFrame({
          'firstdaygain' : [df_strategy1_detected['gainfirstday'].mean()],
          'gainafter5day' : [df_strategy1_detected['gainafter5day'].mean()],          
          'gainafter10day' : [df_strategy1_detected['gainafter10day'].mean()],           
          'gainafter15day' : [df_strategy1_detected['gainafter15day'].mean()],           
          'gainafter20day' : [df_strategy1_detected['gainafter20day'].mean()],           
          'gainafter25day' : [df_strategy1_detected['gainafter25day'].mean()],           
          'gainafter30day' : [df_strategy1_detected['gainafter30day'].mean()],
          
          #df_strategy1_detected['gainafter30day'].mean(), 
          #df_strategy1_detected['gainafter35day'].mean(), 
          #df_strategy1_detected['gainafter40day'].mean(), 
          #df_strategy1_detected['gainafter45day'].mean()
        })  

resultSTD = pd.DataFrame({
        'firstdaygain' : [df_strategy1_detected['gainfirstday'].std()],
        'gainafter5day' : [df_strategy1_detected['gainafter5day'].std()],
        'gainafter10day' : [df_strategy1_detected['gainafter10day'].std()],
        'gainafter15day' : [df_strategy1_detected['gainafter15day'].std()], 
        'gainafter20day' : [df_strategy1_detected['gainafter20day'].std()],
        'gainafter25day' : [df_strategy1_detected['gainafter25day'].std()],
        'gainafter30day' : [df_strategy1_detected['gainafter30day'].std()], 
        
        }) 

result = pd.concat([resultMean,resultSTD], ignore_index=True)

resultGainRatio = pd.DataFrame({
        'firstdaygain' : [df_strategy1_detected['gainfirstday'].mean()/df_strategy1_detected['gainfirstday'].std()],
        'gainafter5day' : [df_strategy1_detected['gainafter5day'].mean()/df_strategy1_detected['gainafter5day'].std()],
        'gainafter10day' : [df_strategy1_detected['gainafter10day'].mean()/df_strategy1_detected['gainafter10day'].std()],
        'gainafter15day' : [df_strategy1_detected['gainafter15day'].mean()/df_strategy1_detected['gainafter15day'].std()], 
        'gainafter20day' : [df_strategy1_detected['gainafter20day'].mean()/df_strategy1_detected['gainafter20day'].std()],
        'gainafter25day' : [df_strategy1_detected['gainafter25day'].mean()/df_strategy1_detected['gainafter25day'].std()],
        'gainafter30day' : [df_strategy1_detected['gainafter30day'].mean()/df_strategy1_detected['gainafter30day'].std()], 
        
        }) 

result = pd.concat([result,resultGainRatio], ignore_index=True)

resultIDs = df_strategy1_detected['id'].unique()


#df_strategy1_detected.sort_values(by='gainafter5day', ascending=True).reset_index(drop=True)

