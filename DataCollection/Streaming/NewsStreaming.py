# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 23:42:09 2020

@author: Hetal
"""
"""
Importing all the necessary libraries
"""


"""
List of papers to crawl from
"""

from newspaper import Article
import newspaper
import pandas as pd
import re
from datetime import datetime
from langdetect import detect
from bloomfilter import BloomFilter
import socket
import concurrent.futures
import time
import requests
from bs4 import BeautifulSoup
papers_dict = {
    'Reuters': {
        'URL': 'https://www.reuters.com/finance/markets',
    },
    'WSJ': {
        'URL': 'https://www.wsj.com/news/economy',
    }
}
"""
URL to crawl from to get company specific news
"""
URL = "https://www.google.com/search?q={query}&tbm=nws&source=lnt&tbs=qdr:h&sa=X&ved=0ahUKEwi6gZqGiIrpAhU4JDQIHVNvA-AQpwUIIw&biw=1536&bih=754&dpr=1.25"
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
queries = ["apple"]


"""
Creating instance of bloom filter class
"""

filter = BloomFilter(5000, 0.2)


def downloadContent(url):
    try:
        print("Downloading from..", url)
        if not(filter.exists(url)):
            if(re.search('www.reuters.com/article/', url) or re.search('www.wsj.com/articles/', url)):
                article = Article(url)
                article.download()
                article.parse()
                if(detect(article.text) != 'en'):
                    return pd.DataFrame()
                publishdate = str(article.publish_date).split(" ")[0]
                if(datetime.today().strftime('%Y-%m-%d') == publishdate):
                    if(len(article.text) > 350 and len(article.text) < 3000):
                        return pd.DataFrame({'URL': [article.url], 'News': [article.text], 'Type': ['Market News'], 'Date': [publishdate]})
                    else:
                        filter.add(url)
            else:
                filter.add(url)
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def downloadPaper(paper):
    # Creating multiple threads to crawl simultaneously
    newsData = pd.DataFrame()
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        jobs = {executor.submit(downloadContent, article.url)
                                : article.url for article in paper.articles}
        for future in concurrent.futures.as_completed(jobs):
            if count > 10:
                break
            try:
                data = future.result()
                if not(data.empty):
                    count += 1
                    filter.add(jobs[future])
                    newsData = pd.concat(
                        [data, newsData], ignore_index=True, sort=False)
            except Exception as exc:
                print('generated an exception: %s' % (exc))
    return newsData


def getNewsData():
    response = pd.DataFrame()
    for name, value in papers_dict.items():
        print("Crawling from: ", value['URL'])
        paper = newspaper.build(
            value['URL'], language='en', memoize_articles=False)
        dataset = downloadPaper(paper)
        response = pd.concat([dataset, response],
                             ignore_index=True, sort=False)

    return response


def downloadCompanyContent(url):
    try:
        if not(filter.exists(url)):
            filter.add(url)
            article = Article(url)
            article.download()
            article.parse()
            if(detect(article.text) != 'en'):
                return ""
            if(len(article.text) > 350 and len(article.text) < 6000):
                publishdate = datetime.today().strftime('%Y-%m-%d')
                print("Downloading from..", url, " publish data ", publishdate)
                return [article.text, publishdate]
            return ""
        else:
            return ""
    except Exception:
        print("in exc")
        return ""


def downloadCompanyNews(urls, query):
    # Creating multiple threads to crawl simultaneously
    newsData = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        jobs = {executor.submit(downloadCompanyContent,
                                url): url for url in urls}
        for future in concurrent.futures.as_completed(jobs):
            try:
                data = future.result()
                if data != "":
                    filter.add(jobs[future])
                    df = pd.DataFrame(
                        {'URL': jobs[future], 'News': data[0], 'Type': query, 'Date': data[1]}, index=[0])
                    newsData = pd.concat(
                        [df, newsData], ignore_index=True, sort=False)
            except Exception as exc:
                print('generated an exception: %s' % (exc))
    return newsData


def loadCompanydata():
    for query in queries:
        session = requests.session()
        response = session.get(URL.format(query=query), headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        divs = soup.findAll("div", {"class": "dbsr"})
        newsUrls = []
        for div in divs:
            url = div.find("a")["href"]
            newsUrls.append(url)
        return downloadCompanyNews(newsUrls, query)


"""
Creating a local TCP Connection
"""
TCP_IP = '127.0.0.1'
TCP_PORT = 5000


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
print("Awaiting Connection")

s.listen(1)
print("Server started at port 5000.")
while True:
    conn, addr = s.accept()
    print("Connected to: ", addr)
    response = loadCompanydata()
    for index, row in response.iterrows():
        print(row['URL'], "-----------", index)
        conn.send((row.to_json()+'\n').encode())
        time.sleep(10)

    response = getNewsData()
    for index, row in response.iterrows():
        print(row['URL'], "-----------", index)
        conn.send((row.to_json()+'\n').encode())
        time.sleep(10)
    time.sleep(10)
    conn.close()
