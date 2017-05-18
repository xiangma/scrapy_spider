# -*- coding: utf-8 -*-
from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import log
from maijiajob.items import JobItem
import datetime,re,sys
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import os,urllib

__author__ = 'fly'

city_code = {
'巢湖':'18#205',
'滁州':'18#202',
'六安':'18#206',
'铜陵':'18#199',
'蚌埠':'18#195',
'安庆':'18#200',
'芜湖':'18#194',
'合肥':'18#193',
'天津':'35',
'重庆':'37',
'北京':'34',
'上海':'36',
'南宁':'38#402',
'宁德':'19#218',
'湖州':'17#186',
'嘉兴':'17#185',
'温州':'17#184',
'绍兴':'17#187',
'宁波':'17#183',
'金华':'17#188',
'丽水':'17#192',
'杭州':'17#182',
'丽江':'29#347',
'曲靖':'29#343',
'大理':'29#354',
'昆明':'29#342',
'乌鲁木齐':'42#440',
'拉萨':'40#428',
'德阳':'27#316',
'宜宾':'27#323',
'达州':'27#325',
'泸州':'27#315',
'南充':'27#322',
'绵阳':'27#317',
'乐山':'27#321',
'成都':'27#312',
'宝鸡':'30#360',
'咸阳':'30#361',
'西安':'30#358',
'临汾':'12#131',
'长治':'12#125',
'阳泉':'12#124',
'运城':'12#129',
'大同':'12#123',
'太原':'12#122',
'菏泽':'21#246',
'枣庄':'21#233',
'滨州':'21#245',
'日照':'21#240',
'济宁':'21#238',
'青岛':'21#231',
'烟台':'21#235',
'潍坊':'21#236',
'淄博':'21#232',
'威海':'21#237',
'泰安':'21#239',
'临沂':'21#242',
'聊城':'21#244',
'东营':'21#234',
'济南':'21#230',
'德州':'21#243',
'西宁':'32#382',
'银川':'41#435',
'赤峰':'39#419',
'鄂尔多斯':'39#421',
'包头':'39#417',
'呼和浩特':'39#416',
'葫芦岛':'13#146',
'铁岭':'13#144',
'营口':'13#140',
'辽阳':'13#142',
'抚顺':'13#136',
'鞍山':'13#135',
'大连':'13#134',
'锦州':'13#139',
'沈阳':'13#133',
'四平':'14#149',
'通化':'14#151',
'吉林':'14#148',
'长春':'14#147',
'泰州':'16#180',
'盐城':'16#177',
'镇江':'16#179',
'南通':'16#174',
'徐州':'16#171',
'扬州':'16#178',
'常州':'16#172',
'无锡':'16#170',
'苏州':'16#173',
'南京':'16#169',
'南昌':'20#219',
'益阳':'24#285',
'邵阳':'24#281',
'衡阳':'24#280',
'岳阳':'24#282',
'株洲':'24#278',
'常德':'24#283',
'长沙':'24#277',
'荆门':'23#270',
'十堰':'23#267',
'宜昌':'23#269',
'襄阳':'23#266',
'荆州':'23#268',
'武汉':'23#264',
'信阳':'22#261',
'三门峡':'22#258',
'开封':'22#248',
'安阳':'22#254',
'南阳':'22#259',
'许昌':'22#256',
'新乡':'22#253',
'平顶山':'22#250',
'洛阳':'22#249',
'郑州':'22#247',
'大庆':'15#161',
'齐齐哈尔':'15#157',
'哈尔滨':'15#156',
'衡水':'11#121',
'廊坊':'11#120',
'邢台':'11#115',
'唐山':'11#112',
'秦皇岛':'11#113',
'邯郸':'11#114',
'承德':'11#118',
'沧州':'11#119',
'保定':'11#116',
'石家庄':'11#111',
'三亚':'26#310',
'海口':'26#309',
'贵阳':'28#333',
'兰州':'31#368',
'肇庆':'25#300',
'阳江':'25#305',
'梅州':'25#302',
'揭阳':'25#457',
'汕尾':'25#303',
'东莞':'25#307',
'江门':'25#297',
'珠海':'25#293',
'深圳':'25#292',
'佛山':'25#296',
'中山':'25#308',
'汕头':'25#294',
'惠州':'25#301',
'广州':'25#291',
'柳州':'38#403',
'漳州':'19#215',
'三明':'19#213',
'泉州':'19#214',
'南平':'19#216',
'龙岩':'19#217',
'厦门':'19#211',
'福州':'19#210',
'马鞍山':'18#197',
'淮南':'18#196',
'桂林':'38#404',

}

quanzhi_category_url = {
    #全职--------------------------------------
    #客服
    '配货'     : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E9%85%8D%E8%B4%A7&page=1',
    '打包发货' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%89%93%E5%8C%85%E5%8F%91%E8%B4%A7&page=1',
    '打单'     : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%89%93%E5%8D%95&page=1',
    '快递员'   : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E5%BF%AB%E9%80%92%E5%91%98&page=1',
    '仓库管理' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E4%BB%93%E5%BA%93%E7%AE%A1%E7%90%86&page=1',
    '网站推广' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&page=1',
    '网络营销' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E7%BD%91%E7%BB%9C%E8%90%A5%E9%94%80&page=1',
    '推广主管' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%8E%A8%E5%B9%BF%E4%B8%BB%E7%AE%A1&page=1',
    'seo'      : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=SEO&page=1',
    '推广专员' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%8E%A8%E5%B9%BF%E4%B8%93%E5%91%98&page=1',
    '钻展推广' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E9%92%BB%E5%B1%95%E6%8E%A8%E5%B9%BF&page=1',
    '活动推广' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%B4%BB%E5%8A%A8%E6%8E%A8%E5%B9%BF&page=1',
    '运营总监' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91&page=1',
    '运营主管' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1&page=1',
    '运营助理' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E8%BF%90%E8%90%A5%E5%8A%A9%E7%90%86&page=1',
    '文案策划' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%96%87%E6%A1%88%E7%AD%96%E5%88%92&page=1',
    '微信运营' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5&page=1',
    '活动策划' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92&page=1',
    '微博运营' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E5%BE%AE%E5%8D%9A%E8%BF%90%E8%90%A5&page=1',
    '网店店长' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E7%BD%91%E5%BA%97%E5%BA%97%E9%95%BF&page=1',
    '运营专员' : 'http://www.chinahr.com/sou?industry=1001%2C1006&work_type=1&orderField=relate&keyword=%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98&page=1',
    '电商运营' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5&page=1',
    '天猫店长' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF&page=1',
    '淘宝店长' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF&page=1',
    '网店运营' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E7%BD%91%E5%BA%97%E8%BF%90%E8%90%A5&page=1',
    '模特'     : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=relate&keyword=%E6%A8%A1%E7%89%B9&page=1',
    '摄影师'   : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E6%91%84%E5%BD%B1%E5%B8%88&page=1',
    '设计总监' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=relate&keyword=%E8%AE%BE%E8%AE%A1%E6%80%BB%E7%9B%91&page=1',
    '设计主管' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=relate&keyword=%E8%AE%BE%E8%AE%A1%E4%B8%BB%E7%AE%A1&page=1',
    '视觉设计' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E8%A7%86%E8%A7%89%E8%AE%BE%E8%AE%A1&page=1',
    '资深美工' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E8%B5%84%E6%B7%B1%E7%BE%8E%E5%B7%A5&page=1',
    '美工设计' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E7%BE%8E%E5%B7%A5%E8%AE%BE%E8%AE%A1&page=1',
    '网页设计' : 'http://www.chinahr.com/sou?work_type=1&orderField=relate&keyword=%E7%BD%91%E9%A1%B5%E8%AE%BE%E8%AE%A1&page=1',

    '淘宝客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '天猫客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '网店客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '电商客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%94%B5%E5%95%86%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '售前客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%94%AE%E5%89%8D%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '售后客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D&page=1&refreshTime=30',
    '客服主管' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%AE%A2%E6%9C%8D%E4%B8%BB%E7%AE%A1&page=1&refreshTime=30',
    '客服经理' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%AE%A2%E6%9C%8D%E7%BB%8F%E7%90%86&page=1&refreshTime=30',
    '客服专员' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%AE%A2%E6%9C%8D%E4%B8%93%E5%91%98&page=1&refreshTime=30',
    #美工设计
    '淘宝美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E7%BE%8E%E5%B7%A5&page=1&refreshTime=30',
    '天猫美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E7%BE%8E%E5%B7%A5&page=1&refreshTime=30',
    '网店美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E7%BE%8E%E5%B7%A5&page=1&refreshTime=30',
    '平面设计' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1&page=1&refreshTime=30',
    '网店设计' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E8%AE%BE%E8%AE%A1&page=1&refreshTime=30',
    #营销推广
    '淘宝推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF&page=1&refreshTime=30',
    '天猫推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF&page=1&refreshTime=30',
    '直通车手' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%9B%B4%E9%80%9A%E8%BD%A6&page=1&refreshTime=30',
    '网店推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF&page=1&refreshTime=30',
    '网站推广/SEO' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&page=1&refreshTime=30',
    '网络营销' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E7%BB%9C%E8%90%A5%E9%94%80&page=1&refreshTime=30',

    #运营管理
    '淘宝店长' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF&page=1&refreshTime=30',
    '天猫店长' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF&page=1&refreshTime=30',
    '网店店长' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E5%BA%97%E9%95%BF&page=1&refreshTime=30',
    '运营管理' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E8%BF%90%E8%90%A5%E7%AE%A1%E7%90%86&page=1&refreshTime=30',
    '网店运营' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E8%BF%90%E8%90%A5&page=1&refreshTime=30',
    '电商运营' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5&page=1&refreshTime=30',
    '运营主管' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1&page=1&refreshTime=30',
    '运营总监' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91&page=1&refreshTime=30',
    '运营专员' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98&page=1&refreshTime=30',

    #仓储物流
    '仓库管理' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E4%BB%93%E5%BA%93%E7%AE%A1%E7%90%86&page=1&refreshTime=30',
    '快递员'   : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E5%BF%AB%E9%80%92%E5%91%98&page=1&refreshTime=30',

    '摄影师'   : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%91%84%E5%BD%B1%E5%B8%88&page=1&refreshTime=30',
    '模特'     : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%A8%A1%E7%89%B9&page=1&refreshTime=30',

    '文案编辑' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%96%87%E6%A1%88%E7%BC%96%E8%BE%91&page=1&refreshTime=30',
    '文案策划' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%96%87%E6%A1%88%E7%AD%96%E5%88%92&page=1&refreshTime=30',

    '网络编辑' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E7%BD%91%E7%BB%9C%E7%BC%96%E8%BE%91&page=1&refreshTime=30',
    '活动策划' : 'http://www.chinahr.com/sou?industry=1001&work_type=1&orderField=reftime&keyword=%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92&page=1&refreshTime=30',

    # 12 - 13 加入的
    '客户关系管理'  :  'http://www.chinahr.com/sou?keyword=%E5%AE%A2%E6%88%B7%E5%85%B3%E7%B3%BB%E7%AE%A1%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '配货打包'      :  'http://www.chinahr.com/sou?keyword=%E9%85%8D%E8%B4%A7%E6%89%93%E5%8C%85&companyType=0&degree=0&refreshTime=2&workAge=0',
    '视觉总监'      :  'http://www.chinahr.com/sou?keyword=%E8%A7%86%E8%A7%89%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    'UI设计'        :  'http://www.chinahr.com/sou?keyword=UI%E8%AE%BE%E8%AE%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    '美工助理'      :  'http://www.chinahr.com/sou?keyword=%E7%BE%8E%E5%B7%A5%E5%8A%A9%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '版师'          :  'http://www.chinahr.com/sou?keyword=%E7%89%88%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '版型师'        :  'http://www.chinahr.com/sou?keyword=%E7%89%88%E5%9E%8B%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '化妆师'        :  'http://www.chinahr.com/sou?keyword=%E5%8C%96%E5%A6%86%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '彩妆师'        :  'http://www.chinahr.com/sou?keyword=%E5%BD%A9%E5%A6%86%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '品牌经理'      :  'http://www.chinahr.com/sou?keyword=%E5%93%81%E7%89%8C%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0#',
    '采购经理'      :  'http://www.chinahr.com/sou?keyword=%E9%87%87%E8%B4%AD%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '天猫运营'      :  'http://www.chinahr.com/sou?keyword=%E5%A4%A9%E7%8C%AB%E8%BF%90%E8%90%A5&companyType=0&degree=0&refreshTime=2&workAge=0',
    '淘宝运营'      :  'http://www.chinahr.com/sou?keyword=%E6%B7%98%E5%AE%9D%E8%BF%90%E8%90%A5&companyType=0&degree=0&refreshTime=2&workAge=0',
    '京东运营'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%AC%E4%B8%9C%E8%BF%90%E8%90%A5&companyType=0&degree=0&refreshTime=2&workAge=0',
    '跨境电商运营'  :  'http://www.chinahr.com/sou?keyword=%E8%B7%A8%E5%A2%83%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5&companyType=0&degree=0&refreshTime=2&workAge=0',
    '店长助理'      :  'http://www.chinahr.com/sou?keyword=%E5%BA%97%E9%95%BF%E5%8A%A9%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '网站编辑'      :  'http://www.chinahr.com/sou?keyword=%E7%BD%91%E7%AB%99%E7%BC%96%E8%BE%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '活动策划'      :  'http://www.chinahr.com/sou?keyword=%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92&companyType=0&degree=0&refreshTime=2&workAge=0',
    '品牌策划'      :  'http://www.chinahr.com/sou?keyword=%E5%93%81%E7%89%8C%E7%AD%96%E5%88%92&companyType=0&degree=0&refreshTime=2&workAge=0',
    '采购员'        :  'http://www.chinahr.com/sou?keyword=%E9%87%87%E8%B4%AD%E5%91%98&companyType=0&degree=0&refreshTime=2&workAge=0',
    '新媒体运营'    :  'http://www.chinahr.com/sou?keyword=%E6%96%B0%E5%AA%92%E4%BD%93%E8%BF%90%E8%90%A5&companyType=0&degree=0&refreshTime=2&workAge=0',
    '销售总监'      :  'http://www.chinahr.com/sou?keyword=%E9%94%80%E5%94%AE%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '市场总监'      :  'http://www.chinahr.com/sou?keyword=%E5%B8%82%E5%9C%BA%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '网络推广'      :  'http://www.chinahr.com/sou?keyword=%E7%BD%91%E7%BB%9C%E6%8E%A8%E5%B9%BF&companyType=0&degree=0&refreshTime=2&workAge=0',
    'SEM'           :  'http://www.chinahr.com/sou?keyword=SEM&companyType=0&degree=0&refreshTime=2&workAge=0',
    '数据分析'      :  'http://www.chinahr.com/sou?keyword=%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90&companyType=0&degree=0&refreshTime=2&workAge=0',
    '销售经理'      :  'http://www.chinahr.com/sou?keyword=%E9%94%80%E5%94%AE%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '客户经理'      :  'http://www.chinahr.com/sou?keyword=%E5%AE%A2%E6%88%B7%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '电话销售'      :  'http://www.chinahr.com/sou?keyword=%E7%94%B5%E8%AF%9D%E9%94%80%E5%94%AE&companyType=0&degree=0&refreshTime=2&workAge=0',
    '市场推广'      :  'http://www.chinahr.com/sou?keyword=%E5%B8%82%E5%9C%BA%E6%8E%A8%E5%B9%BF&companyType=0&degree=0&refreshTime=2&workAge=0',
    '市场营销'      :  'http://www.chinahr.com/sou?keyword=%E5%B8%82%E5%9C%BA%E8%90%A5%E9%94%80&companyType=0&degree=0&refreshTime=2&workAge=0',
    '市场策划'      :  'http://www.chinahr.com/sou?keyword=%E5%B8%82%E5%9C%BA%E7%AD%96%E5%88%92&companyType=0&degree=0&refreshTime=2&workAge=0',
    '总经理'        :  'http://www.chinahr.com/sou?keyword=%E6%80%BB%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '行政经理'      :  'http://www.chinahr.com/sou?keyword=%E8%A1%8C%E6%94%BF%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '人事总监'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%BA%E4%BA%8B%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    'CFO'           :  'http://www.chinahr.com/sou?keyword=CFO&companyType=0&degree=0&refreshTime=2&workAge=0',
    '财务总监'      :  'http://www.chinahr.com/sou?keyword=%E8%B4%A2%E5%8A%A1%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '前台'          :  'http://www.chinahr.com/sou?keyword=%E5%89%8D%E5%8F%B0&companyType=0&degree=0&refreshTime=2&workAge=0',
    '行政助理'      :  'http://www.chinahr.com/sou?keyword=%E8%A1%8C%E6%94%BF%E5%8A%A9%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '总经理助理'    :  'http://www.chinahr.com/sou?keyword=%E6%80%BB%E7%BB%8F%E7%90%86%E5%8A%A9%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '文秘'          :  'http://www.chinahr.com/sou?keyword=%E6%96%87%E7%A7%98&companyType=0&degree=0&refreshTime=2&workAge=0',
    '秘书'          :  'http://www.chinahr.com/sou?keyword=%E7%A7%98%E4%B9%A6&companyType=0&degree=0&refreshTime=2&workAge=0',
    '人事行政'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%BA%E4%BA%8B%E8%A1%8C%E6%94%BF&companyType=0&degree=0&refreshTime=2&workAge=0',
    '人事专员'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%BA%E4%BA%8B%E4%B8%93%E5%91%98&companyType=0&degree=0&refreshTime=2&workAge=0',
    '人事经理'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%BA%E4%BA%8B%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '人事主管'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%BA%E4%BA%8B%E4%B8%BB%E7%AE%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    '出纳'          :  'http://www.chinahr.com/sou?keyword=%E5%87%BA%E7%BA%B3&companyType=0&degree=0&refreshTime=2&workAge=0',
    '会计'          :  'http://www.chinahr.com/sou?keyword=%E4%BC%9A%E8%AE%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    '总账'          :  'http://www.chinahr.com/sou?keyword=%E6%80%BB%E8%B4%A6&companyType=0&degree=0&refreshTime=2&workAge=0',
    '审计'          :  'http://www.chinahr.com/sou?keyword=%E5%AE%A1%E8%AE%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    '法务'          :  'http://www.chinahr.com/sou?keyword=%E6%B3%95%E5%8A%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    '产品经理'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%A7%E5%93%81%E7%BB%8F%E7%90%86&companyType=0&degree=0&refreshTime=2&workAge=0',
    '产品总监'      :  'http://www.chinahr.com/sou?keyword=%E4%BA%A7%E5%93%81%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '架构师'        :  'http://www.chinahr.com/sou?keyword=%E6%9E%B6%E6%9E%84%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '项目经理'      :  'http://www.chinahr.com/sou?keyword=%E9%A1%B9%E7%9B%AE%E7%BB%8F%E7%90%86&industrys=1001&companyType=0&degree=0&refreshTime=2&workAge=0',
    '技术总监'      :  'http://www.chinahr.com/sou?keyword=%E6%8A%80%E6%9C%AF%E6%80%BB%E7%9B%91&companyType=0&degree=0&refreshTime=2&workAge=0',
    '测试主管'      :  'http://www.chinahr.com/sou?keyword=%E6%B5%8B%E8%AF%95%E4%B8%BB%E7%AE%A1&companyType=0&degree=0&refreshTime=2&workAge=0',
    'web前端'       :  'http://www.chinahr.com/sou?keyword=web%E5%89%8D%E7%AB%AF&companyType=0&degree=0&refreshTime=2&workAge=0',
    'html5'         :  'http://www.chinahr.com/sou?keyword=html5&companyType=0&degree=0&refreshTime=2&workAge=0',
    'IOS'           :  'http://www.chinahr.com/sou?keyword=ios&companyType=0&degree=0&refreshTime=2&workAge=0',
    'Android'       :  'http://www.chinahr.com/sou?keyword=Android&companyType=0&degree=0&refreshTime=2&workAge=0',
    'java'          :  'http://www.chinahr.com/sou?keyword=JAVA&companyType=0&degree=0&refreshTime=2&workAge=0',
    'PHP'           :  'http://www.chinahr.com/sou?keyword=PHP&companyType=0&degree=0&refreshTime=2&workAge=0',
    '软件测试'      :  'http://www.chinahr.com/sou?keyword=%E8%BD%AF%E4%BB%B6%E6%B5%8B%E8%AF%95&companyType=0&degree=0&refreshTime=2&workAge=0',
    '测试工程师'    :  'http://www.chinahr.com/sou?keyword=%E6%B5%8B%E8%AF%95%E5%B7%A5%E7%A8%8B%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '网络工程师'    :  'http://www.chinahr.com/sou?keyword=%E7%BD%91%E7%BB%9C%E5%B7%A5%E7%A8%8B%E5%B8%88&companyType=0&degree=0&refreshTime=2&workAge=0',
    '运维'          :  'http://www.chinahr.com/sou?keyword=%E8%BF%90%E7%BB%B4&companyType=0&degree=0&refreshTime=2&workAge=0',
}

shixi_category_url = {
    #实习--------------------------------------
    '客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=3&orderField=reftime&keyword=%E5%AE%A2%E6%9C%8D&page=1',
    #美工设计
    '美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=3&orderField=reftime&keyword=%E7%BE%8E%E5%B7%A5&page=1',
}

jianzhi_category_url = {
    #兼职--------------------------------------
    #客服
    '淘宝客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E5%AE%A2%E6%9C%8D&page=1',
    '天猫客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E5%AE%A2%E6%9C%8D&page=1',
    '网店客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E5%AE%A2%E6%9C%8D&page=1',
    '电商客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E7%94%B5%E5%95%86%E5%AE%A2%E6%9C%8D&page=1',
    '售前客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%94%AE%E5%89%8D%E5%AE%A2%E6%9C%8D&page=1',
    '售后客服' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D&page=1',
    '客服专员' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%AE%A2%E6%9C%8D%E4%B8%93%E5%91%98&page=1',

    #美工设计
    '淘宝美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E7%BE%8E%E5%B7%A5&page=1',
    '天猫美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E7%BE%8E%E5%B7%A5&page=1',
    '网店美工' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E7%BE%8E%E5%B7%A5&page=1',
    '平面设计' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1&page=1',
    #营销推广
    '淘宝推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF&page=1',
    '天猫推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF&page=1',
    '网店推广' : 'http://www.chinahr.com/sou?industry=1001&work_type=2&orderField=reftime&keyword=%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF&page=1',

}

job_type_dict = {
    1 : u'兼职',
    2 : u'实习',
    3 : u'全职',

}

def pick_xpath(sel, path):
    value = sel.xpath(path).extract()
    if value and value[0].strip():
        return value[0].strip()
    return ''

def pick_xpaths(sel, path,letter=','):
    value = sel.xpath(path).extract()
    if value :
        return letter.join(value).strip()
    return ''

class JobzhycSpider(Spider):
    name = "jobzhyc"
    download_delay = 2
    concurrent_requests = 3
    start_urls = (
         'http://www.chinahr.com/',
    )

    para = ''
    def __init__(self,para = None,*args,**kwargs):
        if para:
            self.para = urllib.unquote(para)

        super(JobzhycSpider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)

    def engine_opened(self):
        log.msg('para:' + str(self.para))

    def stats_spider_closed(self, spider, spider_stats):
        feed_uri = self.settings.get("FEED_URI")
        if feed_uri:
            cmd = "xz -c %s > %s.xz" % (feed_uri, feed_uri)
            ret = os.system(cmd)
            if ret == 0:
                log.msg("compress %s successfully!" % feed_uri)
            else:
                log.msg("compress %s failed" % feed_uri)
        # these statistics will be committed to master and then load to mysql
        # start_time,finish_time,item_count,request_count,response_count,exception_count,status_302_count,log_error_count
        stat = 'STAT%s,%s,%s,%s,%s,%s,%s,%s' % (
            spider_stats['start_time'].isoformat(' ')[0:19],
            spider_stats['finish_time'].isoformat(' ')[0:19],
            spider_stats['item_scraped_count'] if 'item_scraped_count' in spider_stats else 0,
            spider_stats['downloader/request_count'] if 'downloader/request_count' in spider_stats else 0,
            spider_stats['downloader/response_count'] if 'downloader/response_count' in spider_stats else 0,
            spider_stats['downloader/exception_count'] if 'downloader/exception_count' in spider_stats else 0,
            spider_stats['downloader/response_status_count/302'] if 'downloader/response_status_count/302' in spider_stats else 0,
            spider_stats['log_count/ERROR'] if 'log_count/ERROR' in spider_stats else 0)
        print stat # print to slave process

    def parse(self, response):

        if self.para:
            para_cities = self.para.split(',')
            log.msg('para cities : '+ str(para_cities))
            for city in para_cities:
                if city not  in city_code:
                    log.err('Unmatch city! '+'!!'*30+ str(city))
                    continue
                c_code = city_code[city].replace('#','%2C')
                log.msg('city city_code:' + city +'#'+ c_code)
                for key in quanzhi_category_url.iterkeys():
                    category = key
                    url = quanzhi_category_url[key] + '&city='+c_code
                    log.msg('quanzhi url:'+'-'*30+url)
                    yield Request(url,meta={'category':category,'job_type':3,'city':city,},callback=self.parse_zhyc_list)

                for key in shixi_category_url.iterkeys():
                    category = key
                    url = shixi_category_url[key] + '&city='+c_code
                    log.msg('shixi url:'+'-'*30+url)
                    yield Request(url,meta={'category':category,'job_type':2,'city':city,},callback=self.parse_zhyc_list)

                for key in jianzhi_category_url.iterkeys():
                    category = key
                    url = jianzhi_category_url[key] + '&city='+c_code
                    log.msg('jianzhi url:'+'-'*30+url)
                    yield Request(url,meta={'category':category,'job_type':1,'city':city,},callback=self.parse_zhyc_list)


    def parse_zhyc_list(self,response):
        sel = Selector(response)
        log.msg('_'*30+response.url)
        job_list = sel.xpath('//div[@class="resultList"]//div[@class="jobList"]')

        dd_now = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).date()

        for cc in job_list:
            tmp_refreshdate = pick_xpath(cc,'.//*[@class="e2"]/text()')

            try:
                refreshdate = re.findall(r'(\d+-\d+)',tmp_refreshdate)
            except Exception,e:
                log.msg(' get refresh date failed! '+ response.urli + '  '+str(e))
                continue

            if  not  refreshdate: # 控制抓取时间范围  'refreshTime' in response.url or
                url = pick_xpath(cc,'./@data-url')
                if not url:
                    log.msg(' get job url failed! '+ response.url)
                    continue
                yield Request(url,meta=response.meta,cookies={'uNameLoginSeeker':'15605813378'},callback=self.parse_zhyc_detail)

            else:
                log.msg(' crawl stop! time out of range! ' + tmp_refreshdate + ' ' + response.url)
                return
        tmp = u'下一页'
        next_flag = pick_xpath(sel,'//*[@class="pageList"]//a[contains(text(),"%s")]/@href'%tmp)
        if next_flag:
            page_num = re.findall(r'&page=(\d+)',response.url)
            if not page_num:
                log.err('no pagenum in url !' + response.url)
            else:
                page_num = int(page_num[0]) + 1
                next_url = re.sub(r'&page=\d+','&page='+str(page_num),response.url)
                yield Request(next_url,meta=response.meta,callback=self.parse_zhyc_list)

    def parse_zhyc_detail(self,response):
        sel = Selector(response)
        ll = JobItem()

        ll['city'] = response.meta['city']
        ll['url'] = response.url
        ll['prop'] = job_type_dict[response.meta['job_type']]
        ll['category'] = response.meta['category']
        ll['name'] = pick_xpath(sel,'//*[@class="job_name"]/text()')
        ll['salary'] = pick_xpath(sel,'//div[@class="job_require"]/span[@class="job_price"]/strong/text()')
        ll['education'] = pick_xpath(sel,'//div[@class="job_require"]/span[last()-1]/text()')
        ll['experience'] = pick_xpath(sel,'//div[@class="job_require"]/span[@class="job_exp"]/strong/text()|//div[@class="job_require"]/span[@class="job_exp"]/text()').strip().strip(u'经验').strip()

        ll['address'] = pick_xpath(sel,'//div[@class="job_require"]/span[@class="job_loc"]/strong/text()|//div[@class="job_require"]/span[@class="job_loc"]/text()')
        ll['welfarelabel'] = pick_xpaths(sel,'//ul[@class="job_fit_tags"]//li[not(@class="s_more on")]/text()',',')[:120] #福利标签 ，没有class属性的li
        ll['publish'] = pick_xpath(sel,'//*[@class="updatetime"]/text()').replace(u'更新','').strip()
        dd_tmp = re.findall(r'(\d+-\d+)',ll['publish'])
        if dd_tmp:
            dd = datetime.datetime.utcnow()+datetime.timedelta(hours=8)
            ll['publish'] = str(dd.year) + str(dd_tmp[0])
  
        ll['desc'] = pick_xpaths(sel,'//*[@class="job_intro_info"]//text()','<br>')                     #职位描述
        
        #from scrapy.shell import inspect_response
        #inspect_response(response)

        if response.meta['job_type'] == 1:
            ll['desc'] = ll['desc'] + '\n' + ll['salary']
            ll['salary'] = ''

        tmp = u'性别'
        ll['gender'] = pick_xpath(sel,'//*[@class="job_intro_tag"]/span[contains(text(),"%s")]/text()'%tmp).replace(u'性别要求：','').strip()
        #----------公司信息
        tmp = u'联系人'
        ll['contact_name'] = pick_xpath(sel,'//div[@class="job-company  mt15 jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).replace(u'联系人：','').strip()
        tmp = u'手机'
        tmp_str1 = pick_xpath(sel,'//div[@class="job-company  mt15 jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).replace(u'手机号码：','').strip()
        tmp = u'固话'
        tmp_str2 = pick_xpath(sel,'//div[@class="job-company  mt15 jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).replace(u'联系电话：','').strip()
        tmp = u'邮箱'
        tmp_str3 = pick_xpath(sel,'//div[@class="job-company  mt15 jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).replace(u'电子邮箱：','').strip()
        
        ll['mailbox'] = tmp_str3
        ll['contact'] = tmp_str1 if tmp_str1 else tmp_str2       #z联系方式
        if (not ll['contact']) and (not ll['mailbox']):  #没有联系人或联系方式则返回
            log.msg('No contact ! '+ '#'+ll['contact_name']+'--#--'+ll['contact']+'#'+response.url)
            return
        tmp = u'地址'
        ll['shopaddr'] = pick_xpath(sel,'//div[@class="job-company  mt15 jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).replace(u'工作地址：','').strip()
        ll['company'] = pick_xpath(sel,'//*[@class="job-company jrpadding"]/h4/a/text()')   #公司名称

        tmp = u'行业'
        ll['industry'] = pick_xpath(sel,'//div[@class="job-company jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).strip('"').strip()
        tmp = u'主页'
        ll['comprop'] = pick_xpath(sel,'//div[@class="job-company jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).strip('"').strip()
        tmp = u'规模'
        ll['scale'] = pick_xpath(sel,'//div[@class="job-company jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).strip('"').strip()
        tmp = u'主页'
        ll['shopurl'] = pick_xpath(sel,'//div[@class="job-company jrpadding"]//tr/td[contains(text(),"%s")]/following-sibling::td[1]/text()'%tmp).strip('"').strip()
        #公司简介
        ll['comprofile'] = pick_xpath(sel,'//div[@class="company_service"]/text()')
        ll['origin'] = 'chinahr'
        yield ll







