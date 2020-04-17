# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 23:42:09 2020

@author: Hetal
"""
"""
Importing all the necessary libraries
"""
from newspaper import Article
import newspaper
import pandas as pd
import re
from datetime import datetime
import socket
import concurrent.futures
import time

"""
List of papers to crawl from
"""
papers_dict = {   
    'Reuters': {
        'URL':'https://www.reuters.com/finance/markets',
    }
}

def downloadContent(url):
    try:
        print("Downloading from..",url)
        if( re.search('www.reuters.com/article/',url) or re.search('www.wsj.com/articles/',url)):
            #print("url---",url)
            article = Article(url)
            article.download()
            article.parse()
            publishdate = str(article.publish_date).split(" ")[0]
            if(datetime.today().strftime('%Y-%m-%d')==publishdate):
                if(len(article.text) > 350 and len(article.text)<3000):
                    return pd.DataFrame({'URL':[article.url],'News':[article.text],'Type':['Market News'],'Date':[publishdate]})
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def downloadPaper(paper):
    #Creating multiple threads to crawl simultaneously
    newsData =pd.DataFrame()
    count =0
    print("In paper ",count)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        print("In executor")
        jobs = {executor.submit(downloadContent, article.url):article.url for article in paper.articles}
        for future in concurrent.futures.as_completed(jobs):
            print("In Jobs count value ",count)
            if count>10:
                break
            try:
                data = future.result()
                if not(data.empty):
                    count+=1
                    newsData = pd.concat([data, newsData], ignore_index=True, sort = False)
            except Exception as exc:
                print('generated an exception: %s' % (exc))               
    return newsData
   
def getNewsData():
    response = pd.DataFrame()
    for name,value in papers_dict.items():
        print("Crawling from: ",value['URL'])
        paper = newspaper.build(value['URL'], language = 'en',memoize_articles=False)
        print(len(paper.articles))
        dataset = downloadPaper(paper)
        response = pd.concat([dataset, response], ignore_index=True, sort = False)
    
    return response

"""
Creating a local TCP Connection
"""
TCP_IP = '127.0.0.1'
TCP_PORT =5000


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
print("Awaiting Connection")

s.listen(1)
print("Server started at port 5000.")
while True:
    conn, addr = s.accept()
    print("Connected to: ",addr)
    response = getNewsData()
    for index,row in response.iterrows():
        print(row['URL'],"-----------",index)
        conn.send((row.to_json()+'\n').encode())
        time.sleep(10)
    time.sleep(10)
    conn.close()
