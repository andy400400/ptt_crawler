# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup 
import pandas as pd
import pymssql
import time
import datetime


class PTTCrawler():
    
    #偽裝瀏覽器
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'}
    
    global day_end
    
    def __init__(self, days):
        tStart = time.time()#計時開始
        
        #抓取今日至幾天前的資料
        self.day_end = (datetime.date.today() - datetime.timedelta(days)).strftime("%Y/%m/%d")
        
        #預設帶入截止時間
        self.dateString = self.day_end
        self.start()
        
        tEnd = time.time()#計時結束
        print('time : ' + str(tEnd - tStart))#原型長這樣
    
    def start(self):
        count = 1
        day_in_range = True
        previous_page = 'index'
        
        #所有明細資料
        all_push_data = pd.DataFrame()
        all_title_data = pd.DataFrame()
        #title資料
        subUrl = []
        title = []
        author = []
        date_array = []
        last_push_time = []
        update_time = []
        #內文統計
        push = []
        arrow = []
        boo = []
        
        while day_in_range == True:
            
            #網頁擷取
            soup = self.get_html(previous_page)
            previous_page = soup.select("div.btn-group-paging a")[1]["href"].strip('/bbs/Stock/').strip('.html')
            sel = soup.select(".r-ent") #取HTML標中的 <div class="title"></div> 中的<a>標籤存入sel
            
            #逐筆取得文章資訊
            for x in reversed(sel):
                        
                #若為公告不抓取資料
                article_title = x.select('div.title')[0].text.strip('\n')
                if article_title.find('[公告]') > -1:
                    continue
                
                #若取不到文章連結代表為刪除文章
                try :
                    article_id = x.select('div.title a')[0]["href"].strip('/bbs/Stock/').strip('.html')
                except IndexError:
                    continue    
                
                #get文章內容
                soup = self.get_html(article_id)
                
                #非公告文章and取不到日期 -> 使用上一篇文章日期
                try :
                    #文章時間處理
                    dateString = soup.select("span.article-meta-value")[3].text
                    dateFormatter = "%a %b %d %H:%M:%S %Y"
                    article_date = datetime.datetime.strptime(dateString, dateFormatter).strftime('%Y/%m/%d')
                except Exception as e:
                    print(e)
                    
                #條件，第一頁不做時間的判定，公告文章也不抓取
                if (count > 1) & (article_date < self.day_end):
                    day_in_range = False 
                    break
                elif (count == 1) & (article_date < self.day_end):
                    break
                
                push_data = self.parse(soup, article_id)
                
                #處除重複資料(依文章、tag、使用者)
                unique_data = push_data.loc[:,'url':'userId'].drop_duplicates()
                
                #如果沒有任何推文，放上即時時間
                try :
                    last_push_time.append(push_data['push_time'].values[-1])
                except:
                    last_push_time.append(datetime.datetime.now().strftime("%m/%d %H:%M"))
                
                #放入標題(title、author、date)，推文統計
                subUrl.append(article_id)
                date_array.append(article_date)
                title.append(article_title)
                author.append(x.select('div.author')[0].text)
                update_time.append(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))        
                push.append(len(unique_data[unique_data.tag=='推']))
                arrow.append(len(unique_data[unique_data.tag=='→']))
                boo.append(len(unique_data[unique_data.tag=='噓']))
                
                #組dataframe
                data = pd.DataFrame({'url':subUrl, 'date':date_array, 'title':title, 'push':push, 'arrow':arrow, 'boo':boo, 'last_push_time':last_push_time, 'update_time':update_time})
                
                #加入
                all_push_data = all_push_data.append(push_data)
                all_title_data = all_title_data.append(data)
        
            count += 1
        
        #寫入資料庫
        self.insert_db(data, all_push_data)

    #get文章內容
    def get_html(self, page_id):
        title_url = "https://www.ptt.cc/bbs/Stock/" + page_id + ".html"   
        html = requests.get(title_url, headers= self.headers) #將此頁面的HTML GET下來
        soup = BeautifulSoup(html.text,"html.parser") #將網頁資料以html.parser
        return soup
    
    def parse(self, soup, article_id):
        #清空變數計算文章內訊息變數
        url = []
        tag = []
        userId = []
        push_content = []
        push_time = []
        
        #擷取推文與計算
        sel_Push = soup.select("div.push span.push-tag")
        sel_UserId = soup.select("div.push span.push-userid")
        sel_push_content = soup.select("div.push span.push-content")
        sel_push_time = soup.select("div.push span.push-ipdatetime")
        
        #取出推文
        for index in range(len(sel_UserId)):
            url.append(article_id)
            tag.append(sel_Push[index].text.strip())
            userId.append(sel_UserId[index].text.strip())
            push_content.append(sel_push_content[index].text.strip().strip(':'))
            push_time.append(sel_push_time[index].text.strip('\n').strip())
    
        #計算推文數
        push_data = pd.DataFrame({'url':url, 'tag':tag, 'userId':userId, 'push_content':push_content, 'push_time':push_time})
        return push_data
    
    
    #寫入資料庫
    def insert_db(self, data, all_push_data):
        sql_conn = pymssql.connect(
          server='',
          user='',
          password='',
          database=''
        )     
        try :
            self.insert_title_data(sql_conn, data)
            self.insert_push_data(sql_conn, all_push_data)
            
        finally:
            sql_conn.close()
    
    #寫入主檔資料
    def insert_title_data(self, sql_conn, data):    
        for index,row in data.iterrows():
            try :
                cursor = sql_conn.cursor()
                cursor.execute('INSERT INTO [TEST].[dbo].[ptt_title] ([url],[title],[date],[push],[arrow],[boo],[last_push_time],[update_time])values (%s, %s, %s, %s, %s, %s, %s, %s)'
                               , (row['url'], row['title'], row['date'], row['push'], row['arrow'], row['boo'], row['last_push_time'], row['update_time'])) 
                sql_conn.commit()
            except Exception as e:
                print(e)
      
    #寫入明細資料
    def insert_push_data(self, sql_conn, all_push_data):
        for index,row in all_push_data.iterrows():
            try :
                cursor = sql_conn.cursor()
                cursor.execute('INSERT INTO  [TEST].[dbo].[ptt_article] ([url],[tag],[user_id],[msg])values (%s, %s, %s, %s)'
                               , (row['url'], row['tag'], row['userId'], row['push_content'])) 
                sql_conn.commit()
            except Exception as e:
                print(e)
        
    