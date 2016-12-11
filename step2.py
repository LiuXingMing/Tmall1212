# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：处理appid
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import requests
import re
import json
import time
import pymongo
from multiprocessing import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_shops = db['Tmall_shops']
collection_items = db['Tmall_items']
collection_appid = db['Tmall_appIDs']


def parse(content, routine):
    js = json.loads(content)
    s = 0

    # 解析里面的商店信息
    try:
        aa = js.values()[0]
        bb = aa['data']
        if len(bb) > 0 and 'extList' in bb[0].keys():
            bb = bb[0]['extList']
        for elem in bb:
            if 'shopUrl' in elem.keys():
                keyName = 'shopUrl'
            elif 'shopActUrl' in elem.keys():
                keyName = 'shopActUrl'
            elif 'mbannerUrl' in elem.keys():
                keyName = 'mbannerUrl'
            elif 'itemUrl' in elem.keys():
                if 'com/item\.htm' in elem['itemUrl']:
                    continue
                keyName  = 'itemUrl'
            else:
                continue
            try:
                s += 1
                if elem[keyName].startswith('//'):
                    collection_shops.insert({'_id': 'https:' + elem[keyName], 'Type': routine['Type']})
                else:
                    collection_shops.insert({'_id': elem[keyName], 'Type': routine['Type']})
            except Exception, e:
                pass
    except Exception, e:
        print 'js error'

    # 解析里面的商品信息
    items = re.findall('com/item\.htm[^"]*id=(\d+)', content)
    for elem in list(set(items)):
        try:
            collection_items.insert({'_id': elem, 'Type': routine['Type']})
        except Exception, e:
            pass

    return [len(set(items)), s]  # 返回解析的数量


def run(routine):
    url = 'https://ald.taobao.com/recommend2.htm?appId=%s&terminalType=1&_pvuuid=%s&source=huichang' % (routine['_id'], str(time.time()) + '000')
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
        except Exception, e:
            print e
            failure += 1
            continue
        i, s = parse(r.content.decode('gbk', 'ignore'), routine)
        print 'Successful: %s(Items:%s; Shops:%s)' % (url, i, s)
        break
    if failure >= 10:
        print 'Failed: %s' % url


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_appid.find())
    pool.close()
    pool.join()

    # run({'_id': 'lb-zebra-211303-1630287', 'Type': '21jfdiew'})
