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


# response = requests.get('https://www.bilibili.com')
# print(response.cookies)
# for key,value in response.cookies.items():
#     print(key,'==',value)


def fetch_secondary_data(avid, rpid, word):
    """
    Fetch secondary data from rpid

    """
    target2 = f'https://api.bilibili.com/x/v2/reply/reply?&type=1&oid={avid}&root={rpid}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    while True:
        try:
            req = requests.get(url=target2, headers=headers, timeout=0.5)
            if req is None or req.status_code != requests.codes.ok:
                print(f'Error! Position 3.Type {req.status_code}')
                if req.status_code == 412:
                    # print(target2)
                    time.sleep(600)
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
                        # print(target2)
                        time.sleep(600)
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    while True:
        try:
            req = requests.get(url=target1, headers=headers, timeout=0.5)
            if req is None or req.status_code != requests.codes.ok:
                print(f'Error! Position 3.Type {req.status_code}')
                if req.status_code == 412:
                    # print(target2)
                    time.sleep(600)
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
                    if req.status_code == 412:
                        # print(target2)
                        time.sleep(600)
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


# accumulation = primary_comments + secondary_comments
# print(accumulation)


def store_data(contents, avid):
    """
    Store data fetched as proper format: Convert lists to a Dataframe and save it as CSV

    """
    df = pd.DataFrame(contents)
    df.columns = ['key', 'avid', 'account_number', 'account_name', 'sex', 'comments_content', 'like',
                  'rcount']
    # print(df)

    df.to_csv(f'D:/{avid}.csv', index=False, encoding='utf_8_sig')
    # df.to_csv('D:/test1.csv',index=False)


def main():
    # Fill your av ID list here
    file = pd.read_excel('D:/lda.xlsx', sheet_name=1)
    # file = file[:1]
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
