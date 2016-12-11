# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：抓取商店信息
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import pymongo
import requests
import re
from lxml import etree
from multiprocessing import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_shops = db['Tmall_shops']
collection_items = db['Tmall_items']
collection_items_temp = db['Tmall_items_temp']


def parse(content, sourceURL, routine):
    try:
        text = content.replace('&#x2F;', '/').replace('&quot;', '"').replace('&amp;', '&')

        # 解析出json的URL
        tree = etree.HTML(text)
        site_instance_id = re.findall('site_instance_id=(\d+)', text)
        data_widgetid = tree.xpath('//div[@class="J_TModule J_TAsyncModule"]/@data-widgetid')
        flag = 0
        if site_instance_id:
            if (site_instance_id[0] + '-/p/shj.htm') in text:   # 复杂的贱货
                for elem in data_widgetid:
                    if int(elem) % 2 == 0:
                        continue
                    host = re.findall('//([^/]*)', sourceURL)
                    if host:
                        url = 'https://' + host[0] + '/widgetAsync.htm?ids=' + elem + '%2C' + str(int(elem) + 1) + \
                              '&path=%2Fp%2Fshj.htm&callback=callbackGetMods' + elem + '&site_instance_id=' + site_instance_id[0]
                        try:
                            flag += 1
                            collection_items_temp.insert({'_id': url, 'ShopURL': sourceURL, 'Type': routine['Type']})
                        except Exception, e:
                            pass
                else:
                    print 'No host'
            else:
                for elem in data_widgetid:
                    host = re.findall('//([^/\?]*)', sourceURL)
                    if host:
                        url = 'https://' + host[0] + '/widgetAsync.htm?ids=' + elem + '&path=%2Fshop%2Fview_shop.htm&callback=callbackGetMods' + \
                              elem + '&site_instance_id=' + site_instance_id[0]
                        try:
                            flag += 1
                            collection_items_temp.insert({'_id': url, 'ShopURL': sourceURL, 'Type': routine['Type']})
                        except Exception, e:
                            pass
                    else:
                        print 'No host'

        # 解析商品ID
        items = re.findall('com/item\.htm[^"]*id=(\d+)', text)
        for elem in list(set(items)):
            try:
                collection_items.insert({'_id': elem, 'ShopURL': sourceURL, 'Type': routine['Type']})
            except Exception, e:
                pass
        return [flag, len(set(items))]
    except Exception, e:
        print e
        return [0, 0]


def run(routine):
    url = routine['_id']
    if url.startswith('//'):
        url = 'https:' + url
    failure = 0
    while failure < 10:
        try:
            r = requests.get(url, timeout=10)
        except Exception, e:
            print e
            failure += 1
            continue
        temp, items = parse(r.content.decode('gbk', 'ignore'), url, routine)
        print 'Successful: %s (Temp:%s; Items:%s)' % (routine['_id'], temp, items)
        break
    if failure >= 10:
        print 'Failed: %s' % url


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_shops.find())
    pool.close()
    pool.join()
