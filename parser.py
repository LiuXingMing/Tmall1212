# encoding=utf-8
# ----------------------------------------------------------------------
#   作用：解析crawl_detail.py抓取到的页面
#   日期：2016-12-12
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ----------------------------------------------------------------------

import sys

reload(sys)
sys.setdefaultencoding('utf8')
import time
import pymongo
import re
import json
from lxml import etree
from multiprocessing.dummy import Pool, cpu_count

client = pymongo.MongoClient('localhost', 27017)
db = client['1212']
collection_html = db['Tmall_details']
collection_imgs = db['Tmall_detail_imgs']
collection_result = db['Tmall_result']
collection_result1 = db['Tmall_source1']
collection_result2 = db['Tmall_source2']
collection_failure = db['Tmall_detail_failure']


def run(routine):
    try:
        img_routines = []
        detail0 = []
        tree = etree.HTML(routine['Content'])
        data_detail = re.findall('_DATA_Detail = *?\n?(.*?\});? ?\n', routine['Content'])
        data_mdskip = re.findall('_DATA_Mdskip = *?\n?(.*?\});? ?\n', routine['Content'])
        data_detail_js = json.loads(data_detail[0])
        data_mdskip_js = json.loads(data_mdskip[0])

        # 商品标题
        title = tree.xpath('//section[@id="s-title"]/div[@class="main"]/h1/text()')
        if title:
            title = title[0]
        else:
            title = tree.xpath('//section[@id="s-title"]/div[@class="main"]/h1/text()')
            if title:
                title = title[0].replace(' - 天猫Tmall.com', '')
            else:
                title = ''

        # 一个商品下可能有颜色、码数选择，不同的选择会有不同的照片
        if 'valItemInfo' in data_detail_js.keys() and 'skuPics' in data_detail_js['valItemInfo'].keys():
            for key in data_detail_js['valItemInfo']['skuPics'].keys():
                try:
                    value = data_detail_js['valItemInfo']['skuPics'][key]
                    if key.startswith(';'):
                        key = key[1:]
                    if key.endswith(';'):
                        key = key[:-1]
                    key = 'https://detail.tmall.com/item.htm?id=%s&sku_properties=%s' % (
                        routine['_id'], key.replace(';', '&'))
                    if value.startswith('//'):
                        value = 'http:' + value
                    elif value.startswith('/'):
                        value = 'http:/' + value
                    elif not value.startswith('http'):
                        value = 'http://' + value
                    img_routines.append({'_id': value, '商品链接': key, '商品标题': title})
                except Exception, e:
                    print e
        # 网页下拉，商品介绍时显示的照片
        if 'api' in data_detail_js.keys() and 'newWapDescJson' in data_detail_js['api'].keys():
            for one in data_detail_js['api']['newWapDescJson']:
                if 'moduleName' in one.keys() and one['moduleName'] == '商品图片' and 'data' in one.keys():
                    for elem in one['data']:
                        try:
                            temp = {'_id': elem['img']}
                            if 'width' in elem.keys():
                                temp['width'] = elem['width']
                            if 'height' in elem.keys():
                                temp['height'] = elem['height']
                            temp['商品链接'] = 'https://detail.tmall.com/item.htm?id=%s' % routine['_id']
                            temp['商品标题'] = title
                            img_routines.append(temp)
                        except Exception, e:
                            print e
        for img in img_routines:
            try:
                collection_imgs.insert(img)
            except Exception, e:
                pass

        # 服务保障
        fuwu = []
        if 'defaultModel' in data_mdskip_js.keys() and 'consumerProtection' in data_mdskip_js[
            'defaultModel'].keys() and 'items' in data_mdskip_js[
            'defaultModel']['consumerProtection'].keys():
            for one in data_mdskip_js['defaultModel']['consumerProtection']['items']:
                if 'title' in one.keys():
                    fuwu.append(one['title'])
            fuwu = ';'.join(fuwu)

        # 优惠活动
        youhui = []
        if 'defaultModel' in data_mdskip_js.keys() and 'couponDataDo' in data_mdskip_js[
            'defaultModel'].keys() and 'couponList' in data_mdskip_js[
            'defaultModel']['couponDataDo'].keys():
            for one in data_mdskip_js['defaultModel']['couponDataDo']['couponList']:
                if 'title' in one.keys() and one['title'] != '领取优惠券':
                    youhui.append(one['title'])
            youhui = ';'.join(youhui)
            youhui = youhui.replace('.', '点')
        elif 'defaultModel' in data_mdskip_js.keys() and 'itemPriceResultDO' in data_mdskip_js[
            'defaultModel'].keys() and 'tmallShopProm' in \
                data_mdskip_js['defaultModel']['itemPriceResultDO'].keys():
            for one in data_mdskip_js['defaultModel']['itemPriceResultDO']['tmallShopProm']:
                if 'promPlanMsg' in one.keys():
                    youhui = ';'.join(one['promPlanMsg'])
                    youhui = youhui.replace('.', '点')

        # 卖家地址及快递费:
        maijiadizhi = ''
        kuaidifei = ''
        if 'defaultModel' in data_mdskip_js.keys() and 'deliveryDO' in data_mdskip_js[
            'defaultModel'].keys() and 'deliverySkuMap' in data_mdskip_js[
            'defaultModel']['deliveryDO'].keys():
            temp = data_mdskip_js['defaultModel']['deliveryDO']['deliverySkuMap']
            if 'default' in temp.keys():
                for one in temp['default']:
                    if 'postage' in one.keys() and len(one['postage']) > 0:
                        kuaidifei = one['postage']
                    if 'skuDeliveryAddress' in one.keys() and len(one['skuDeliveryAddress']) > 0:
                        maijiadizhi = one['skuDeliveryAddress']

        # 以上为不同颜色/型号商品共享的数据，以下求每个颜色/型号的商品信息
        if 'defaultModel' in data_mdskip_js.keys() and 'itemPriceResultDO' in data_mdskip_js[
            'defaultModel'].keys() and 'priceInfo' in data_mdskip_js[
            'defaultModel']['itemPriceResultDO'].keys():
            for elem in data_mdskip_js['defaultModel']['itemPriceResultDO']['priceInfo'].keys():
                value = data_mdskip_js['defaultModel']['itemPriceResultDO']['priceInfo'][elem]
                temp = {'_id': 'https://detail.tmall.com/item.htm?id=%s&skuId=%s' % (routine['_id'], elem)}
                if fuwu:
                    temp['服务保障'] = fuwu
                if youhui:
                    temp['优惠活动'] = youhui
                if maijiadizhi:
                    temp['卖家地址'] = maijiadizhi
                if kuaidifei:
                    temp['快递费'] = kuaidifei
                if 'tagPrice' in value.keys() and len(value['tagPrice']) > 0:
                    temp['原价'] = value['tagPrice']
                elif 'price' in value.keys() and len(value['price']) > 0:
                    temp['原价'] = value['price']
                if 'promotionList' in value.keys():
                    for one in value['promotionList']:
                        if 'price' in one.keys() and len(one['price']) > 0:
                            temp['现价'] = one['price']
                        if 'startTime' in one.keys():
                            temp['活动开始时间'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(one['startTime'] / 1000))
                        elif 'tradeResult' in data_mdskip_js['defaultModel'].keys() and 'startTime' in \
                                data_mdskip_js['defaultModel'][
                                    'tradeResult'].keys():
                            startTime = data_mdskip_js['defaultModel']['tradeResult']['startTime']
                            temp['活动开始时间'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startTime / 1000))
                        if 'endTime' in one.keys():
                            temp['活动结束时间'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(one['endTime'] / 1000))
                temp['标题'] = title
                if 'Type' in routine.keys():
                    temp['会场'] = routine['Type']
                if 'ShopUrl' in routine.keys():
                    temp['商店链接'] = routine['ShopUrl']
                temp['商品ID'] = routine['_id']
                detail0.append(temp)
        for item in detail0:
            try:
                collection_result.insert(item)
            except Exception, e:
                print e

        try:
            data_detail_js['_id'] = routine['_id']
            collection_result1.insert(data_detail_js)
        except Exception, e:
            print e
        try:
            data_mdskip_js['_id'] = routine['_id']
            collection_result2.insert(data_mdskip_js)
        except Exception, e:
            print e
        with open('ids.txt', 'a') as f:
            f.write('%s\n' % routine['_id'])
        print 'Finish %s' % routine['_id']
    except Exception, e:
        print e
        try:
            collection_failure.insert(routine)
        except Exception, e:
            print e


if __name__ == '__main__':
    pool = Pool(cpu_count())
    pool.map(run, collection_html.find())
    pool.close()
    pool.join()
