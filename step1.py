# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：抓取各个会场页面上的商品、商店、appid等重要信息
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import pymongo
import requests
import re
from multiprocessing import Pool, cpu_count
from urldict import urldict

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_shops = db['Tmall_shops']
collection_items = db['Tmall_items']
collection_appid = db['Tmall_appIDs']
collection_tec = db['Tmall_tecs']


def parse(content, sourceURL):
    try:
        text = content.replace('&#x2F;', '/').replace('&quot;', '"').replace('&amp;', '&')
        shopURL = re.findall('shopActUrl":"(.*?)"', text)
        itemURL0 = re.findall('itemId":"(\d+)"', text)  # 有两种方式解析商品id
        itemURL1 = re.findall('item\.htm\?id=(\d+)', text)
        appid = re.findall('"appId":"(.*?)","terminalType', text)
        tec = re.findall('"tce_sid":(\d+)', text)
        others = re.findall('"itemUrl":"(.*?)"', text)
        for one in others:
            temp = re.findall('[\?&]id=(\d+)', one)
            if temp:
                itemURL0.append(temp[0])
            else:
                shopURL.append(one)

        # 以下将各信息入库
        for elem in list(set(shopURL)):
            try:
                if elem.startswith('//'):
                    elem = 'https:' + elem
                collection_shops.insert({'_id': elem, 'Type': urldict[sourceURL]})
            except Exception, e:
                # print 'shops:' % e
                pass
        for elem in list(set(itemURL0 + itemURL1)):
            try:
                collection_items.insert({'_id': elem, 'Type': urldict[sourceURL]})
            except Exception, e:
                # print 'items: %s' % e
                pass
        for elem in list(set(appid)):
            try:
                collection_appid.insert({'_id': elem, 'Type': urldict[sourceURL]})
            except Exception, e:
                # print 'appid: %s' % e
                pass

        for elem in list(set(tec)):
            try:
                collection_tec.insert({'_id': elem, 'Type': urldict[sourceURL]})
            except Exception, e:
                # print 'tec: %s' % e
                pass
        return [len(set(shopURL)), len(set(itemURL0 + itemURL1)), len(set(appid)), len(set(tec))]   # 返回各数量
    except Exception, e:
        print '!!!!!!!!!!!!!!!!!!!'


def run(url):
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
        except Exception, e:
            print e
            failure += 1
            continue
        shops, items, appid, tec = parse(r.content, url)
        print 'Successful: %s (Shops:%s; Items:%s; AppID:%s; Tec:%s)' % (url, shops, items, appid, tec)
        break
    if failure >= 10:
        print 'Failed: %s' % url


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, urldict.keys())
    pool.close()
    pool.join()
