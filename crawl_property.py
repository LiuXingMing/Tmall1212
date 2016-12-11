# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：抓取商品参数信息
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import requests
import json
import pymongo
from multiprocessing import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_itmes = db['Tmall_items']
collection_Tmall_others = db['Tmall_property']


def run(routine):
    sid = routine['_id']
    url = 'https://mdetail.tmall.com/mobile/itemPackage.do?itemId=%s' % sid
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
            js = json.loads(r.content.decode('gbk', 'ignore'))
        except Exception, e:
            print e
            failure += 1
            continue
        result = {'_id': sid}
        if 'model' in js.keys() and 'list' in js['model'].keys():
            for one in js['model']['list']:
                if 'v' in one.keys():
                    for elem in one['v']:
                        if 'k' in elem.keys() and 'v' in elem.keys():
                            result[elem['k']] = elem['v']
        if len(result.keys()) == 1:
            print 'None: %s' % sid
            with open('failure.txt', 'a') as f:
                f.write('%s None\n' % sid)
        else:
            try:
                print 'Finish: %s' % sid
                collection_Tmall_others.insert(result)
            except Exception, e:
                print e
        break
    if failure >= 10:
        print 'Failed: %s' % sid
        with open('failure.txt', 'a') as f:
            f.write('%s erroe\n' % sid)


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_itmes.find())
    pool.close()
    pool.join()
