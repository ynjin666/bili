#!/usr/bin/env python
import requests
from requests.exceptions import ReadTimeout, ConnectionError, RequestException
import json
import pandas as pd
import numpy as np
import math
import time
import random


def random_ip():
    """
    Get ip randomly

    :return: proxy
    """

    http_proxies = ['70.169.141.35:3128', '47.89.153.213:80', '3.95.126.46:80', '3.216.45.64:80', '104.211.29.96:80',
                    '92.204.129.161:80', '134.122.113.7:8080', '191.252.61.219:3128', '198.13.55.233:3128',
                    '212.179.18.75:3128', '77.68.29.157:80', '35.172.135.29:80', '167.99.59.236:8080',
                    '189.206.105.164:80',
                    '78.47.16.54:80']
    i = random.randint(0, len(http_proxies) - 1)
    proxies = {'http': http_proxies[i]}
    return proxies


def fetch_from_api(target, slow, quick, headers):
    """
    Retry until get data from API
    :return:request.txt
    """
    while True:
        try:
            req = requests.get(url=target, headers=headers, timeout=0.5, proxies=random_ip())
            if req is None or req.status_code != requests.codes.ok:
                print(f'Error! Type {req.status_code}')
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
        except OtherType:
            print('Error!Trying to reconnect……')
            time.sleep(random.choice(quick))
            continue
    re = req.text
    return re


def fetch_secondary_data(avid, rpid, word):
    """
    get relative secondary_comments according to a particular rpid value.
    :param avid: av ID
    :param rpid: a unique code that looks up a piece of primary_comment
    :param word: a keyword that helps us know the video belongs to which type of samples
    :return: a secondary list
    """

    target = f'https://api.bilibili.com/x/v2/reply/reply?&type=1&oid={avid}&root={rpid}'
    headers = {
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    quick = [1, 2, 3, 4, 5]
    slow = [300, 600.100, 150, 60, 400, 200]

    data=fetch_from_api(target, slow, quick, headers)
    hot = json.loads(data)
    page = math.ceil(hot['data']['page']['count'] / 49)  # set the number of comments displayed per page is 49
    global rows1
    rows1 = []

    for page_num in range(1, page + 1):
        # for page_num in range(1, 2):
        target = f'https://api.bilibili.com/x/v2/reply/reply?&type=1&oid={avid}&root={rpid}&pn={page_num}&ps=49'
        data = fetch_from_api(target, slow, quick, headers)
        hot = json.loads(data)  # decode json text to python dictionary

        if 'data' in hot and hot['data']['replies'] is not None:
            for reply in hot['data']['replies']:
                mid = reply['member']['mid']
                uname = reply['member']['uname']
                sex = reply['member']['sex']
                msg = reply['content']['message']
                like = reply['like']
                rcount = reply['rcount']

                rows1.append([avid, word, mid, uname, sex, msg, like, rcount])

    return rows1


def fetch_data(word, avid):
    """Fetch data from specific av ID and page number

    Args:
        avid (str): target av ID
        word: sample ID
    """
    target = f'https://api.bilibili.com/x/v2/reply?&type=1&oid={avid}&sort=1'
    headers = {
        'Cookie': '76f61c00%2C1632195409%2C9dd45*31;8e314273cbe5aca449342fa1a5ded225'}

    quick = [1, 2, 3, 4, 5]
    slow = [300, 600.100, 150, 60, 400, 200]

    data = fetch_from_api(target, slow, quick, headers)
    hot = json.loads(data)
    page_info = hot['data']['page']
    num = page_info['acount']  # all Comment count of an item
    print(f"Total comment count:{num}")  # display the comment counts
    page = math.ceil(page_info['count'] / 49)

    primary_comments = []
    secondary_comments = []

    for page_num in range(1, page + 1):
        print(f'Current page_num:{page_num}')
        target = f'https://api.bilibili.com/x/v2/reply?&type=1&oid={avid}&sort=1&pn={page_num}&ps=49'
        data = fetch_from_api(target, slow, quick, headers)
        hot = json.loads(data)

        for reply in hot['data']['replies']:
            rpid = reply['rpid']
            uname = reply['member']['uname']
            mid = reply['member']['mid']
            sex = reply['member']['sex']
            msg = reply['content']['message']
            like = reply['like']
            rcount = reply['rcount']

            primary_comments.append([avid, word, mid, uname, sex, msg, like, rcount])
            secondary_comments += fetch_secondary_data(avid, rpid, word)

    return primary_comments + secondary_comments


def store_data(contents, avid):
    """
    convert the list to a dataframe and save it as a CSV file
    :param contents: the comments fetched
    :param avid: video ID
    :return: a CSV file
    """
    df = pd.DataFrame(contents)
    df.columns = ['key', 'avid', 'account_number', 'account_name', 'sex', 'comments_content', 'like',
                  'rcount']

    df.to_csv(f'D:/content/{avid}.csv', index=False, encoding='utf_8_sig')


def main():
    file = pd.read_excel('D:/lda.xlsx', sheet_name=1)
    file = file[:1]
    av_list = list(file['id'].values)

    for avid in av_list:
        contents = []
        word1 = file.iloc[np.where(file.id.values == avid)]
        word = word1['key'].values
        print(f'Current av ID: {word},{avid}')

        contents += fetch_data(word, avid)

        print(f"Congratulations! {len(contents)} comments of video {avid} have been scraped.")
        store_data(contents, avid)


main()
