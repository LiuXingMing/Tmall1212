# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：根据商品ID抓取商品页面
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import requests
import pymongo
from multiprocessing import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_items = db['Tmall_items']
collection_items_failure = db['Tmall_items_failure']
collection_details = db['Tmall_details']


def run(routine):
    url = 'https://detail.m.tmall.com/item.htm?id=%s' % routine['_id']
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
        except Exception, e:
            print e
            failure += 1
            continue
        routine['Content'] = r.content.decode('gbk', 'ignore')
        if routine['Content'].startswith('\r\n<!doc'):
            failure = 10
            break
        try:
            collection_details.insert(routine)
        except Exception, e:
            pass
        print 'Successful: %s' % routine['_id']
        break
    if failure >= 10:
        print 'Failed: %s' % routine['_id']
        try:
            collection_items_failure.insert(routine)
        except Exception, e:
            pass


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_items.find())
    pool.close()
    pool.join()
