# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：根据图片信息下载图片，要先在此py的同目录下新建一个文件夹 "IMG"。
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import sys

reload(sys)
sys.setdefaultencoding('utf8')
import urllib
import pymongo
from hashlib import md5
from multiprocessing import Pool

client = pymongo.MongoClient('localhost', 27017)
db = client['1111']
collection_img = db['Tmall_detail_imgs']
collection_img_finished = db['Tmall_detail_imgs_finished']


def run(_):
    try:
        routine = collection_img.find_one_and_delete({})
        url = routine['_id']
        m5 = md5()
        m5.update(url)
        routine['url_md5'] = m5.hexdigest()
        collection_img_finished.insert(routine)
    except Exception, e:
        print e
        return
    if url.endswith('jpg'):
        img_dir = './IMG/%s.jpg' % m5.hexdigest()
    else:
        img_dir = './IMG/%s.png' % m5.hexdigest()
    failure = 0
    while failure < 10:
        try:
            urllib.urlretrieve(url, img_dir)
            break
        except Exception, e:
            print e
            failure += 1
            continue
    if failure >= 10:
        print 'Failed: %s' % url
        with open('img_failure.txt', 'a') as f:
            f.write('%s\n' % url)


if __name__ == '__main__':
    while collection_img.count() > 0:
        pool = Pool(8)
        pool.map(run, range(10000))
        pool.close()
        pool.join()
        print '一万'
