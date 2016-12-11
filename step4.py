# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：处理step3.py产生的Tmall_items_temp，主要是json的URL，此py获取json，并解析出商品ID
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import pymongo
import requests
import re
from multiprocessing import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_items = db['Tmall_items']
collection_items_temp = db['Tmall_items_temp']


def parse(content, routine):
    try:
        items = re.findall('com/item\.htm[^"]*id=(\d+)', content)
        for elem in list(set(items)):
            try:
                collection_items.insert({'_id': elem, 'ShopURL': routine['ShopURL'], 'Type': routine['Type']})
            except Exception, e:
                pass
        return len(set(items))
    except Exception, e:
        print e
    return 0


def run(routine):
    url = routine['_id']
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
        except Exception, e:
            print e
            failure += 1
            continue
        items = parse(r.content.decode('gbk', 'ignore'), routine)
        print 'Successful: %s (Items:%s)' % (routine['_id'], items)
        break
    if failure >= 10:
        print 'Failed: %s' % url


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_items_temp.find())
    pool.close()
    pool.join()


