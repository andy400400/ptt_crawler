import requests
from bs4 import BeautifulSoup 
import numpy as np
import pandas as pd
import pymssql
import time
from datetime import date

tStart = time.time()#計時開始

#變數
day_end = date.today().strftime(%m%d)
count = 1
day_in_range = True
previous_page = 'index'

#title資料
subUrl = []
title = []
author = []
date_array = []
#內文統計
push = []
arrow = []
boo = []

#偽裝瀏覽器
headers = {'user-agent' 'Mozilla5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit537.36 (KHTML, like Gecko) Chrome66.0.3359.181 Safari537.36'}

while day_in_range == True
    
    #網頁擷取
    title_url = httpswww.ptt.ccbbsStock + previous_page + .html   
    html = requests.get(title_url, headers=headers) #將此頁面的HTML GET下來
    soup = BeautifulSoup(html.text,html.parser) #將網頁資料以html.parser
    previous_page = soup.select(div.btn-group-paging a)[1][href].strip('bbsStock').strip('.html')
    sel = soup.select(.r-ent) #取HTML標中的 div class=titlediv 中的a標籤存入sel
    

    #逐筆取得文章資訊
    for x in reversed(sel)
        #若取不到文章連結代表為刪除文章
        try 
            article_id = x.select('div.title a')[0][href].strip('bbsStock').strip('.html')
        except IndexError
            continue
        
        article_date = str(0) + x.select('div.date')[0].text.strip()
        #條件，第一頁不做時間的判定
        if (count  1) & (article_date  day_end)
            day_in_range = False 
            break
        
        #放入標題(title、author、date)
        subUrl.append(article_id)
        date_array.append(article_date)
        title.append(x.select('div.title')[0].text.strip('n'))
        author.append(x.select('div.author')[0].text)

        #清空變數計算文章內訊息變數
        url = []
        tag = []
        userId = []
        
        article_url = httpswww.ptt.ccbbsStock + article_id + .html
        html = requests.get(article_url, headers=headers) #將此頁面的HTML GET下來
        soup = BeautifulSoup(html.text,html.parser)
        sel_Push = soup.select(div.push span.push-tag)
        sel_UserId = soup.select(div.push span.push-userid)

        #取出推文
        for index in range(len(sel_UserId))
            url.append(article_id)
            tag.append(sel_Push[index].text.strip())
            userId.append(sel_UserId[index].text.strip())
        #計算推文數
        push_data = pd.DataFrame({'url'url, 'tag'tag, 'userId'userId})
        unique_data = push_data.drop_duplicates()
        #資料放入array
        push.append(len(unique_data[unique_data.tag=='推']))
        arrow.append(len(unique_data[unique_data.tag=='→']))
        boo.append(len(unique_data[unique_data.tag=='噓']))
    
    count += 1

#統整成dataFrame
data = pd.DataFrame({'url'subUrl, 'date'date_array, 'title'title, 'push'push, 'arrow'arrow, 'boo'boo})

tEnd = time.time()#計時結束
print(tEnd - tStart)#原型長這樣

data
