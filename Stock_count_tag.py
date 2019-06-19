import requests
from bs4 import BeautifulSoup 
import numpy as np
import pandas as pd
import pymssql
import time
from datetime import date

tStart = time.time()#�p�ɶ}�l

#�ܼ�
day_end = date.today().strftime(%m%d)
count = 1
day_in_range = True
previous_page = 'index'

#title���
subUrl = []
title = []
author = []
date_array = []
#����έp
push = []
arrow = []
boo = []

#�����s����
headers = {'user-agent' 'Mozilla5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit537.36 (KHTML, like Gecko) Chrome66.0.3359.181 Safari537.36'}

while day_in_range == True
    
    #�����^��
    title_url = httpswww.ptt.ccbbsStock + previous_page + .html   
    html = requests.get(title_url, headers=headers) #�N��������HTML GET�U��
    soup = BeautifulSoup(html.text,html.parser) #�N������ƥHhtml.parser
    previous_page = soup.select(div.btn-group-paging a)[1][href].strip('bbsStock').strip('.html')
    sel = soup.select(.r-ent) #��HTML�Ф��� div class=titlediv ����a���Ҧs�Jsel
    

    #�v�����o�峹��T
    for x in reversed(sel)
        #�Y������峹�s���N���R���峹
        try 
            article_id = x.select('div.title a')[0][href].strip('bbsStock').strip('.html')
        except IndexError
            continue
        
        article_date = str(0) + x.select('div.date')[0].text.strip()
        #����A�Ĥ@�������ɶ����P�w
        if (count  1) & (article_date  day_end)
            day_in_range = False 
            break
        
        #��J���D(title�Bauthor�Bdate)
        subUrl.append(article_id)
        date_array.append(article_date)
        title.append(x.select('div.title')[0].text.strip('n'))
        author.append(x.select('div.author')[0].text)

        #�M���ܼƭp��峹���T���ܼ�
        url = []
        tag = []
        userId = []
        
        article_url = httpswww.ptt.ccbbsStock + article_id + .html
        html = requests.get(article_url, headers=headers) #�N��������HTML GET�U��
        soup = BeautifulSoup(html.text,html.parser)
        sel_Push = soup.select(div.push span.push-tag)
        sel_UserId = soup.select(div.push span.push-userid)

        #���X����
        for index in range(len(sel_UserId))
            url.append(article_id)
            tag.append(sel_Push[index].text.strip())
            userId.append(sel_UserId[index].text.strip())
        #�p������
        push_data = pd.DataFrame({'url'url, 'tag'tag, 'userId'userId})
        unique_data = push_data.drop_duplicates()
        #��Ʃ�Jarray
        push.append(len(unique_data[unique_data.tag=='��']))
        arrow.append(len(unique_data[unique_data.tag=='��']))
        boo.append(len(unique_data[unique_data.tag=='�N']))
    
    count += 1

#�ξ㦨dataFrame
data = pd.DataFrame({'url'subUrl, 'date'date_array, 'title'title, 'push'push, 'arrow'arrow, 'boo'boo})

tEnd = time.time()#�p�ɵ���
print(tEnd - tStart)#�쫬���o��

data
