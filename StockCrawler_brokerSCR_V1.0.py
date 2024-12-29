import twstock
import requests as r
import pandas as pd
from bs4 import BeautifulSoup
import datetime
import json
import yfinance
from io import StringIO
import numpy as np
import mysql.connector
import pymysql
from mysql.connector import Error
import numpy as np
from time import sleep
import tkinter as tk
from tkinter import messagebox
import sys
import re
import socket
import random
from function import functions
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import time  # 用於計算請求時間


# 自訂適配器，用於綁定本地 IP
class LocalIPAdapter(HTTPAdapter):
    def __init__(self, local_ip, *args, **kwargs):
        self.local_ip = local_ip
        super(LocalIPAdapter, self).__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['source_address'] = (self.local_ip, 0)  # 綁定本地 IP
        return super(LocalIPAdapter, self).init_poolmanager(*args, **kwargs)


# 資料庫參數設定
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "twstockinfo"
}

connection = pymysql.connect(**db_settings,
                             connect_timeout=60) # 設置超時時間
# 建立游標物件
cursor = connection.cursor()
query = """ SELECT * FROM twstockinfo.twstocktable
WHERE datetime BETWEEN '2023-01-01 00:00:00' AND '2023-12-31 23:59:59';"""
df = pd.read_sql(query, connection) # type -> pandas.core.frame.DataFrame


# 設定本地 IP 清單
local_ips = ["192.168.0.253","192.168.0.252","192.168.0.251","192.168.0.250","192.168.0.249","192.168.0.248","192.168.0.247","192.168.0.246","192.168.0.245","192.168.0.244","192.168.0.243","192.168.0.242","192.168.0.241"]


# 請求 URL
url = "https://jdata.yuanta.com.tw/z/zc/zco/zco.djhtm"

# Headers
headers = {
    "authority": "jdata.yuanta.com.tw",
    "method": "GET",
    #"path": "/z/zc/zco/zco.djhtm?a=3003&e=2024-12-5&f=2024-12-5",
    "scheme": "https",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,eo;q=0.6",
    #"cookie": "_ga=GA1.1.663940139.1729253630; _ga_GJL94PXDCK=GS1.1.1733592359.4.1.1733592806.60.0.0",
    #"referer": "https://jdata.yuanta.com.tw/z/ZC/ZCO/ZCO_3003.djhtm",
    "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "iframe",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}



# 每次請求使用不同的 IP
for i in range(len(df)) : # i =71249
    start_time =time.time()
    connection = pymysql.connect(**db_settings,
                                 connect_timeout=60) # 設置超時時間
    # 建立游標物件
    cursor = connection.cursor()

    # 請求參數
    params = {
        "a": df['id'][i],   # 股票代碼
        "e": functions.date_font(df['datetime'][i],'yuanta'),  # 結束日期
        "f": functions.date_font(df['datetime'][i],'yuanta'),  # 開始日期
    }
    
    ip = random.choice(local_ips)  # 隨機選擇 IP
    session = r.Session()
    session.mount("http://", LocalIPAdapter(ip))
    session.mount("https://", LocalIPAdapter(ip))
    
        
    res = session.get(url, headers=headers, params=params)
    res.encoding = "big5"  # 如果內容是 Big5 編碼
    html_txt = res.text
    
    # 使用 BeautifulSoup 解析 HTML
    soup = BeautifulSoup(html_txt, 'html.parser')
    
    # 找到數據表格
    table = soup.find('table', {'id': 'oMainTable'})
    if table is None :
        continue
    # 初始化數據容器
    buy_data = []
    sell_data = []
    # 遍歷表格行
    try :
        for row in table.find_all('tr')[4:]:  # 跳過前面與表頭無關的部分
            cells = row.find_all('td')
            if len(cells) == 10 :  # 確保是數據行

                buy_data.append([
                    cells[0].get_text(strip=True),  # 買超券商
                    cells[1].get_text(strip=True),  # 買進
                    cells[2].get_text(strip=True),  # 賣出
                    cells[3].get_text(strip=True),  # 買超
                    cells[4].get_text(strip=True)   # 佔成交比重
                ])
                sell_data.append([
                    cells[5].get_text(strip=True),  # 賣超券商
                    cells[6].get_text(strip=True),  # 買進
                    cells[7].get_text(strip=True),  # 賣出
                    cells[8].get_text(strip=True),  # 賣超
                    cells[9].get_text(strip=True)   # 佔成交比重
                ])
                
        buy_df = pd.DataFrame(buy_data[1:], columns=['買超券商', '買進', '賣出', '買超', '佔成交比重'])
        buy_df['佔成交比重'] = buy_df['佔成交比重'].replace('', '0').replace('-', '0').str.rstrip('%').astype(float)
        
        sell_df = pd.DataFrame(sell_data[1:], columns=['賣超券商', '買進', '賣出', '買超', '佔成交比重'])
        sell_df['佔成交比重'] = sell_df['佔成交比重'].replace('', '0').replace('-', '0').str.rstrip('%').astype(float)
        
        SCR = round((buy_df['佔成交比重'].sum()+sell_df['佔成交比重'].sum())/100,3)
        
        
        # 執行 SQL 更新操作，使用 %s 作為參數佔位符
        sql = "UPDATE twstockinfo.twstocktable SET SCR = %s WHERE datetime = %s AND id = %s "
        values = (SCR, df['datetime'][i].to_pydatetime(), df['id'][i])
        
        cursor.execute(sql, values)
        connection.commit()
        
        # 建立 DataFrame
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Finish Stock ID : "+ df['id'][i]+ ' Date: ' + str(df['datetime'][i]) + " 耗時: " + str(round(elapsed_time,4)) + " 秒 " + "使用IP: " + ip )
        
    finally:
        # 确保在所有操作完成后关闭连接
        if connection.open:
            connection.close()








