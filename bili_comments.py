#!/usr/bin/env python
# -*- coding: gbk -*-

# 目前的最大问题：存储的评论数量似乎不对
# 爬取速度好慢，Pool分布式
# 动态cookie? 412错误重出江湖


import requests
from requests.exceptions import ReadTimeout, ConnectionError, RequestException
import json
import pandas as pd
import numpy as np
import math
import time
import random


# response = requests.get('https://www.bilibili.com')
# print(response.cookies)
# for key,value in response.cookies.items():
#     print(key,'==',value)

def GetUserAgent():
    '''
    功能：随机获取HTTP_User_Agent
    function: get random User Agent headers
    '''
    user_agents = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    ]
    user_agent = random.choice(user_agents)
    return user_agent


def random_ip():
    '''
        function: get random ip
    '''

    http_proxies = ['70.169.141.35:3128', '47.89.153.213:80', '3.95.126.46:80', '3.216.45.64:80', '104.211.29.96:80',
                    '92.204.129.161:80', '134.122.113.7:8080', '191.252.61.219:3128', '198.13.55.233:3128',
                    '212.179.18.75:3128', '77.68.29.157:80', '35.172.135.29:80', '167.99.59.236:8080',
                    '189.206.105.164:80',
                    '78.47.16.54:80']
    i = random.randint(0, len(http_proxies) - 1)
    proxies = {'http': http_proxies[i]}
    return proxies


def fetch_secondary_data(avid, rpid, word):
    """
    Fetch secondary data from rpid

    """
    target2 = f'https://api.bilibili.com/x/v2/reply/reply?&type=1&oid={avid}&root={rpid}'
    headers = {
        'User-Agent': GetUserAgent(),
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    quick = [1, 2, 3, 4, 5]
    slow = [300, 600.100, 150, 60, 400, 200]

    while True:
        try:
            req = requests.get(url=target2, headers=headers, timeout=0.5, proxies=random_ip())
            if req is None or req.status_code != requests.codes.ok:
                print(f'Error! Position 3.Type {req.status_code}')
                time.sleep(random.choice(quick))
                if req.status_code == 412:
                    # print(target2)
                    time.sleep(random.choice(slow))
                continue
            break
        except ReadTimeout:
            print('Timeout')
        except ConnectionError:
            print('Connection error')
        except RequestException:
            print('Error')
        except:
            print('Error!Type 3. Trying to reconnect……')
            time.sleep(random.choice(quick))
            continue

    data = req.text

    hot = json.loads(data)
    page = math.ceil(hot['data']['page']['count'] / 49)  # set the number of comments displayed per page is 49
    global rows1
    rows1 = []

    for page_num in range(1, page + 1):
        # for page_num in range(1, 2):

        target = f'https://api.bilibili.com/x/v2/reply/reply?&type=1&oid={avid}&root={rpid}&pn={page_num}&ps=49'

        while True:
            try:
                req = requests.get(url=target, headers=headers, timeout=0.5)
                if req is None or req.status_code != requests.codes.ok:
                    print(f'Error! Position 3.Type {req.status_code}')
                    if req.status_code == 412:
                        time.sleep(random.choice(slow))
                    continue
                break
            except ReadTimeout:
                print('Timeout')
            except ConnectionError:
                print('Connection error')
            except RequestException:
                print('Error')
            except:
                print('Error!Type 4. Trying to reconnect……')
                time.sleep(random.choice(quick))
                continue

        data = req.text  # data is a str
        hot = json.loads(data)  # decode json text to python dictionary

        if 'data' in hot and hot['data']['replies'] is not None:
            for reply in hot['data']['replies']:
                mid = reply['member']['mid']
                uname = reply['member']['uname']
                sex = reply['member']['sex']
                msg = reply['content']['message']
                like = reply['like']
                rcount = reply['rcount']

                # print(f"\t\t{mid},{uname},{sex},{msg},{like},{rcount}")

                rows1.append([avid, word, mid, uname, sex, msg, like, rcount])

    return rows1


def fetch_data(word, avid):
    """Fetch data from specific av ID and page number

    Args:
        avid (str): target av ID
        page_num (str): target page number
    """
    target1 = f'https://api.bilibili.com/x/v2/reply?&type=1&oid={avid}&sort=1'
    headers = {
        'User-Agent': GetUserAgent(),
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    quick = [1, 2, 3, 4, 5]
    slow = [300, 600.100, 150, 60, 400, 200]

    while True:
        try:
            req = requests.get(url=target1, headers=headers, timeout=0.5, proxies=random_ip())
            if req is None or req.status_code != requests.codes.ok:
                print(f'Error! Position 3.Type {req.status_code}')
                time.sleep(random.choice(quick))
                if req.status_code == 412:
                    time.sleep(random.choice(slow))
                continue
            break
        except ReadTimeout:
            print('Timeout')
        except ConnectionError:
            print('Connection error')
        except RequestException:
            print('Error')
        except:
            print('Error!Type 1. Trying to reconnect……')
            time.sleep(random.choice(quick))
            continue

    data = req.text
    hot = json.loads(data)
    page_info = hot['data']['page']
    num = page_info['acount']  # all Comment count of an item
    print(f"Total comment count:{num}")  # display the comment counts
    page = math.ceil(page_info['count'] / 49)
    # print(f'Total page_num is {page}')  # compute the total page_num

    primary_comments = []
    secondary_comments = []

    for page_num in range(1, page + 1):
        # for page_num in range(1, 3):
        print(f'Current page_num:{page_num}')
        target = f'https://api.bilibili.com/x/v2/reply?&type=1&oid={avid}&sort=1&pn={page_num}&ps=49'

        while True:
            try:
                req = requests.get(url=target, headers=headers, timeout=0.5)
                if req is None or req.status_code != requests.codes.ok:
                    print(f'Error! Position 3.Type {req.status_code}')
                    time.sleep(random.choice(quick))
                    if req.status_code == 412:
                        time.sleep(random.choice(slow))
                    continue
                break
            except ReadTimeout:
                print('Timeout')
            except ConnectionError:
                print('Connection error')
            except RequestException:
                print('Error')
            except:
                print('Error!Type 2. Trying to reconnect……')
                time.sleep(random.choice(quick))
                continue

        data = req.text
        hot = json.loads(data)

        for reply in hot['data']['replies']:
            rpid = reply['rpid']
            uname = reply['member']['uname']
            mid = reply['member']['mid']
            sex = reply['member']['sex']
            msg = reply['content']['message']
            like = reply['like']
            rcount = reply['rcount']

            # print(f"{mid},{uname},{sex},{msg},{like},{rcount}")
            primary_comments.append([avid, word, mid, uname, sex, msg, like, rcount])
            secondary_comments += fetch_secondary_data(avid, rpid, word)

    return primary_comments + secondary_comments


def store_data(contents, avid):
    """
    Store data fetched as proper format: Convert lists to a Dataframe and save it as CSV

    """
    df = pd.DataFrame(contents)
    df.columns = ['key', 'avid', 'account_number', 'account_name', 'sex', 'comments_content', 'like',
                  'rcount']
    # print(df)

    df.to_csv(f'D:/content/{avid}.csv', index=False, encoding='utf_8_sig')
    # df.to_csv('D:/test1.csv',index=False)


def main():
    # Fill your av ID list here
    file = pd.read_excel('D:/lda.xlsx', sheet_name=1)
    file = file[32:42]
    avlist = list(file['id'].values)

    for avid in avlist:
        contents = []
        word1 = file.iloc[np.where(file.id.values == avid)]
        word = word1['key'].values
        # word = df[df.id == avid] the result is the same as the above
        print(f'Current av ID: {word},{avid}')

        # get the key's value
        contents += fetch_data(word, avid)

        print(
            f"Congratulations! {len(contents)} comments of video {avid} have been scraped! You are so capable a girl ! (NO, it's the crawler lol.)")
        # print(contents)
        store_data(contents, avid)

    # result = contents
    # contents = [word + c for c in contents]


main()
