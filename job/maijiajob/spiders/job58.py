# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from maijiajob.items import JobItem
from scrapy.selector import Selector
from scrapy import log
import re
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import os
from urllib import unquote
import datetime
from kafka import KafkaConsumer, KafkaProducer

def pyproducer(topic,_key,_value):
    log.msg('sending message!')
    producer = KafkaProducer(bootstrap_servers='115.231.103.59:9092,115.231.103.212:9092,115.231.103.60:9092',retries=3,api_version='0.8.2')
    future = producer.send(topic,key=_key,value=_value)
    try:
        record_metadata = future.get(timeout=10)
    except Exception,e:
        # Decide what to do if produce request failed...
        log.err(str(e))
        pass
    log.msg(record_metadata.topic)
    log.msg(record_metadata.partition)
    log.msg(record_metadata.offset)
    producer.close()

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

job_type_dict = {
    1 : u'兼职',
    2 : u'实习',
    3 : u'全职',

    }

class Job58Spider(scrapy.Spider):
    name = "job58"
    download_delay = 1
    # 城市列表
    start_urls = (
        'http://www.58.com/zptaobao/changecity/',
    )
    
    domain_matcher = r'(http://.*\.com).*'
    date_matcher = r'[0-9]{2}-[0-9]{2}'
    name_matcher = re.compile(r"linkman:'(.{,15})'")
  
    def __init__(self, para=None, *args, **kwargs):
        super(Job58Spider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started) 
        self.para = para

    def engine_opened(self):
        log.msg('para:' + str(self.para))

    def stats_spider_closed(self, spider, spider_stats):
        feed_uri = self.settings.get("FEED_URI")
        if feed_uri:
            cmd = "xz -c %s > %s.xz" % (feed_uri, feed_uri)
            ret = os.system(cmd)
            if ret == 0:
                log.msg("compress %s successfully!" % feed_uri)
                fp = open(feed_uri+'.xz','rb')
                filename = feed_uri.split('/')[-1]
                content = fp.read()
                pyproducer('job',bytes(filename),content)
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
        # 判断是否有参数
        if self.para:
            citys = self.para.split(',')
            for city in citys:
                city_code = city.split(':')[0]
                city_name = city.split(':')[1]
                if city_name.startswith('%'):
                    city_name = unquote(city_name)
                url = 'http://' + city_code + '.58.com/zptaobao/'
                yield Request(url=url, meta={"city": city_name,"city_code":city_code}, callback=self.parse_category) 
        else:
            for selector in response.xpath('//dl[@id="clist"]/dd/a'):
                url = selector.xpath('@href').extract()[0]
                city_name = selector.xpath('text()').extract()[0]
                yield Request(url=url, meta={"city": city_name}, callback=self.parse_category)

    def parse_category(self, response):
        """
         抓取八大类目职位信息： 淘宝客服， 淘宝美工， 店铺推广， 网店店长 , 文案编辑 ， 活动策划 ， 仓库管理 ，快递员
         job_type = 类型 1 2 3  兼职 实习 全职
        """
        city = response.meta["city"]
        city_code = response.meta['city_code']
        # 检查是否被ban
        if '/support.58.com/firewall' in response.url:
            log.msg("Oops, banned !")
            return

        peihuo='http://'+city_code+'.58.com/job/?key=%E9%85%8D%E8%B4%A7&cmcskey=%E9%85%8D%E8%B4%A7&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'配货'
        yield Request(url=peihuo, meta={"category": category, "city": city}, callback=self.parse_job_list)

        dabaofahuo='http://'+city_code+'.58.com/job/?key=%E6%89%93%E5%8C%85%E5%8F%91%E8%B4%A7&cmcskey=%E6%89%93%E5%8C%85%E5%8F%91%E8%B4%A7&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'打包发货'
        yield Request(url=dabaofahuo, meta={"category": category, "city": city}, callback=self.parse_job_list)

        dadian='http://'+city_code+'.58.com/job/?key=%252525E6%25252589%25252593%252525E5%2525258D%25252595&sourcetype=4'
        category=u'打单'
        yield Request(url=dadian, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangzhantuiguang='http://'+city_code+'.58.com/tech/?key=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&cmcskey=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'网站推广'
        yield Request(url=wangzhantuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangluoyingxiao='http://'+city_code+'.58.com/shichang/?key=%E7%BD%91%E7%BB%9C%E8%90%A5%E9%94%80&cmcskey=%E7%BD%91%E7%BB%9C%E8%90%A5%E9%94%80&final=1&specialtype=gls&canclequery=isbiz%3D0&PGTID=125831487189756881083008793&ClickID=2'
        category=u'网络营销'
        yield Request(url=wangluoyingxiao, meta={"category": category, "city": city}, callback=self.parse_job_list)

        tuiguangzhuguan='http://'+city_code+'.58.com/job/?key=%E6%8E%A8%E5%B9%BF%E4%B8%BB%E7%AE%A1&cmcskey=%E6%8E%A8%E5%B9%BF%E4%B8%BB%E7%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'推广主管'
        yield Request(url=tuiguangzhuguan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        seo='http://'+city_code+'.58.com/tech/?key=seo&cmcskey=seo&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'seo'
        yield Request(url=seo, meta={"category": category, "city": city}, callback=self.parse_job_list)

        tuiguangzhuanyuan='http://'+city_code+'.58.com/job/?key=%E6%8E%A8%E5%B9%BF%E4%B8%93%E5%91%98&cmcskey=%E6%8E%A8%E5%B9%BF%E4%B8%93%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'推广专员'
        yield Request(url=tuiguangzhuanyuan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        zuanzhantuiguang='http://'+city_code+'.58.com/job/?key=%E9%92%BB%E5%B1%95%E6%8E%A8%E5%B9%BF&cmcskey=%E9%92%BB%E5%B1%95%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'钻展推广'
        yield Request(url=zuanzhantuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        zhitongche='http://'+city_code+'.58.com/job/?key=%E7%9B%B4%E9%80%9A%E8%BD%A6&cmcskey=%E7%9B%B4%E9%80%9A%E8%BD%A6&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'直通车'
        yield Request(url=zhitongche, meta={"category": category, "city": city}, callback=self.parse_job_list)

        tianmaotuiguang='http://'+city_code+'.58.com/zptaobaotuiguang/?key=%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF&cmcskey=%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'天猫推广'
        yield Request(url=tianmaotuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        taobaotuiguang='http://'+city_code+'.58.com/zptaobaotuiguang/?key=%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF&cmcskey=%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'淘宝推广'
        yield Request(url=taobaotuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        huodongtuiguang='http://hz.58.com/job/?key=%E6%B4%BB%E5%8A%A8%E6%8E%A8%E5%B9%BF&cmcskey=%E6%B4%BB%E5%8A%A8%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'活动推广'
        yield Request(url=huodongtuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        crm='http://'+city_code+'.58.com/job/?key=crm&cmcskey=crm&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'crm'
        yield Request(url=crm, meta={"category": category, "city": city}, callback=self.parse_job_list)

        yunyingzongjian='http://'+city_code+'.58.com/zpguanli/?key=%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91&cmcskey=%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'运营总监'
        yield Request(url=yunyingzongjian, meta={"category": category, "city": city}, callback=self.parse_job_list)

        yunyingzhuguan='http://'+city_code+'.58.com/tech/?key=%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1&cmcskey=%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'运营主管'
        yield Request(url=yunyingzhuguan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        yunyingzhuli='http://'+city_code+'.58.com/job/?key=%E8%BF%90%E8%90%A5%E5%8A%A9%E7%90%86&cmcskey=%E8%BF%90%E8%90%A5%E5%8A%A9%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'运营助理'
        yield Request(url=yunyingzhuli, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wenancehua='http://'+city_code+'.58.com/job/?key=%E6%96%87%E6%A1%88%E7%AD%96%E5%88%92&cmcskey=%E6%96%87%E6%A1%88%E7%AD%96%E5%88%92&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'文案策划'
        yield Request(url=wenancehua, meta={"category": category, "city": city}, callback=self.parse_job_list)

        weixinyunying='http://'+city_code+'.58.com/job/?key=%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5&cmcskey=%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'微信运营'
        yield Request(url=weixinyunying, meta={"category": category, "city": city}, callback=self.parse_job_list)

        weiboyunying='http://'+city_code+'.58.com/job/?key=%E5%BE%AE%E5%8D%9A%E8%BF%90%E8%90%A5&cmcskey=%E5%BE%AE%E5%8D%9A%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'微博运营'
        yield Request(url=weiboyunying, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangdiandianzhang='http://'+city_code+'.58.com/zptaobao/?key=%E7%BD%91%E5%BA%97%E5%BA%97%E9%95%BF&cmcskey=%E7%BD%91%E5%BA%97%E5%BA%97%E9%95%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'网店店长'
        yield Request(url=wangdiandianzhang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        yunyingzhuanyuan='http://'+city_code+'.58.com/job/?key=%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98&cmcskey=%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'运营专员'
        yield Request(url=yunyingzhuanyuan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        dianshangyunying='http://'+city_code+'.58.com/job/?key=%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5&cmcskey=%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'电商运营'
        yield Request(url=dianshangyunying, meta={"category": category, "city": city}, callback=self.parse_job_list)

        tianmaodianzhang='http://'+city_code+'.58.com/zptaobaodianzhang/?key=%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF&cmcskey=%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'天猫店长'
        yield Request(url=tianmaodianzhang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        taobaodianzhang='http://'+city_code+'.58.com/zptaobaodianzhang/?key=%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF&cmcskey=%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'淘宝店长'
        yield Request(url=taobaodianzhang, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangdianyunying='http://'+city_code+'.58.com/zptaobao/?key=%E7%BD%91%E5%BA%97%E8%BF%90%E8%90%A5&cmcskey=%E7%BD%91%E5%BA%97%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'网店运营'
        yield Request(url=wangdianyunying, meta={"category": category, "city": city}, callback=self.parse_job_list)

        shejizongjian='http://'+city_code+'.58.com/zpguanggao/?key=%E8%AE%BE%E8%AE%A1%E6%80%BB%E7%9B%91&cmcskey=%E8%AE%BE%E8%AE%A1%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'设计总监'
        yield Request(url=shejizongjian, meta={"category": category, "city": city}, callback=self.parse_job_list)

        meigongzhuli='http://'+city_code+'.58.com/renli/?key=%E7%BE%8E%E5%B7%A5%E5%8A%A9%E7%90%86&cmcskey=%E7%BE%8E%E5%B7%A5%E5%8A%A9%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'美工助理'
        yield Request(url=meigongzhuli, meta={"category": category, "city": city}, callback=self.parse_job_list)

        zishengmeigong='http://'+city_code+'.58.com/job/?key=%252525E8%252525B5%25252584%252525E6%252525B7%252525B1%252525E7%252525BE%2525258E%252525E5%252525B7%252525A5&sourcetype=4'
        category=u'资深美工'
        yield Request(url=zishengmeigong, meta={"category": category, "city": city}, callback=self.parse_job_list)

        shijuesheji='http://'+city_code+'.58.com/job/?PGTID=14476371277920.3111314042471349&ClickID=8&key=%252525E8%252525A7%25252586%252525E8%252525A7%25252589%252525E8%252525AE%252525BE%252525E8%252525AE%252525A1'
        category=u'视觉设计'
        yield Request(url=shijuesheji, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangyesheji='http://'+city_code+'.58.com/job/?key=%252525E7%252525BD%25252591%252525E9%252525A1%252525B5%252525E8%252525AE%252525BE%252525E8%252525AE%252525A1&sourcetype=4'
        category=u'网页设计'
        yield Request(url=wangyesheji, meta={"category": category, "city": city}, callback=self.parse_job_list)

        pingmiansheji='http://'+city_code+'.58.com/zpmeishu/?key=%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1&cmcskey=%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'平面设计'
        yield Request(url=pingmiansheji, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangdianmeigong='http://'+city_code+'.58.com/job/?key=%252525E7%252525BD%25252591%252525E5%252525BA%25252597%252525E7%252525BE%2525258E%252525E5%252525B7%252525A5&sourcetype=4'
        category=u'网店美工'
        yield Request(url=wangdianmeigong, meta={"category": category, "city": city}, callback=self.parse_job_list)

        kefujingli='http://'+city_code+'.58.com/job/?key=%252525E5%252525AE%252525A2%252525E6%2525259C%2525258D%252525E7%252525BB%2525258F%252525E7%25252590%25252586&sourcetype=4'
        category=u'客服经理'
        yield Request(url=kefujingli, meta={"category": category, "city": city}, callback=self.parse_job_list)

        kefuzhuguan='http://'+city_code+'.58.com/job/?key=%252525E5%252525AE%252525A2%252525E6%2525259C%2525258D%252525E4%252525B8%252525BB%252525E7%252525AE%252525A1&sourcetype=4'
        category=u'客服主管'
        yield Request(url=kefuzhuguan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        kefuzhuanyuan='http://'+city_code+'.58.com/kefuzhuanyuan/?key=%E5%AE%A2%E6%9C%8D%E4%B8%93%E5%91%98&cmcskey=%E5%AE%A2%E6%9C%8D%E4%B8%93%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'客服专员'
        yield Request(url=kefuzhuanyuan, meta={"category": category, "city": city}, callback=self.parse_job_list)

        wangdiankefu='http://'+city_code+'.58.com/kefu/?key=%E7%BD%91%E5%BA%97%E5%AE%A2%E6%9C%8D&cmcskey=%E7%BD%91%E5%BA%97%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'网店客服'
        yield Request(url=wangdiankefu, meta={"category": category, "city": city}, callback=self.parse_job_list)

        dianshangkefu='http://'+city_code+'.58.com/job/?key=%E7%94%B5%E5%95%86%E5%AE%A2%E6%9C%8D&cmcskey=%E7%94%B5%E5%95%86%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'电商客服'
        yield Request(url=dianshangkefu, meta={"category": category, "city": city}, callback=self.parse_job_list)

        shouhoukefu='http://'+city_code+'.58.com/kefu/?key=%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D&cmcskey=%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'售后客服'
        yield Request(url=shouhoukefu, meta={"category": category, "city": city}, callback=self.parse_job_list)

        shouqiankefu='http://'+city_code+'.58.com/kefu/?key=%E5%94%AE%E5%89%8D%E5%AE%A2%E6%9C%8D&cmcskey=%E5%94%AE%E5%89%8D%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'售前客服'
        yield Request(url=shouqiankefu, meta={"category": category, "city": city}, callback=self.parse_job_list)

        tiammaokefu='http://'+city_code+'.58.com/zptaobaokefu/?key=%E5%A4%A9%E7%8C%AB%E5%AE%A2%E6%9C%8D&cmcskey=%E5%A4%A9%E7%8C%AB%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category=u'天猫客服'
        yield Request(url=tiammaokefu, meta={"category": category, "city": city}, callback=self.parse_job_list)

        taobaokefu = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaokefu")]/@href').extract()[0].strip()
        category = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaokefu")]/text()').extract()[0].strip()
        yield Request(url=taobaokefu, meta={"category": category, "city": city}, callback=self.parse_job_list)
        dianzhang = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaodianzhang")]/@href').extract()[0].strip()
        category = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaodianzhang")]/text()').extract()[0].strip()
        yield Request(url=dianzhang, meta={"category": category, "city": city}, callback=self.parse_job_list)
        meigong = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaomeigong")]/@href').extract()[0].strip()
        category = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaomeigong")]/text()').extract()[0].strip()
        yield Request(url=meigong, meta={"category": category, "city": city}, callback=self.parse_job_list)
        tuiguang = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaotuiguang")]/@href').extract()[0].strip()
        category = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaotuiguang")]/text()').extract()[0].strip()
        yield Request(url=tuiguang, meta={"category": category, "city": city}, callback=self.parse_job_list)
        
        wenanbianji = 'http://'+city_code+'.58.com/zptaobaobianji/?PGTID=14334898443900.5265174044761807&ClickID=1'
        category = u'文案编辑'
        yield Request(url=wenanbianji, meta={"category": category, "city": city}, callback=self.parse_job_list)
        
        huodongcehua = 'http://'+city_code+'.58.com/zptaobaocehua/?PGTID=14334900636710.09247982525266707&ClickID=1'
        category = u'活动策划'
        yield Request(url=huodongcehua, meta={"category": category, "city": city}, callback=self.parse_job_list)
        
        cangkuguanli = 'http://'+city_code+'.58.com/zpwuliucangchu/?key=%E4%BB%93%E5%BA%93%E7%AE%A1%E7%90%86&cmcskey=%E4%BB%93%E5%BA%93%E7%AE%A1%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category = u'仓库管理'
        yield Request(url=cangkuguanli, meta={"category": category, "city": city}, callback=self.parse_job_list)
        
        kuaidiyuan = 'http://'+city_code+'.58.com/job/?key=%E5%BF%AB%E9%80%92%E5%91%98&cmcskey=%E5%BF%AB%E9%80%92%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        category = u'快递员'
        yield Request(url=kuaidiyuan, meta={"category": category, "city": city}, callback=self.parse_job_list)
        
        category = u'摄影师'
        job_type = 3
        url = 'http://'+city_code+'.58.com/job/?key=%E6%91%84%E5%BD%B1%E5%B8%88&sourcetype=1_4&PGTID=147353264189439431924830855&ClickID=1'
        yield Request(url=url,meta={"category": category, "city": city,'job_type':job_type},callback=self.parse_job_list)

        category = u'平面模特'
        job_type = 3
        url = 'http://'+city_code+'.58.com/job/?key=%E5%B9%B3%E9%9D%A2%E6%A8%A1%E7%89%B9&cmcskey=%E5%B9%B3%E9%9D%A2%E6%A8%A1%E7%89%B9&final=1&jump=2&specialtype=gls&pgtid=147353264189439431924830855&clickid=1&canclequery=isbiz%3D0&sourcetype=1_4'
        yield Request(url=url,meta={"category": category, "city": city,'job_type':job_type},callback=self.parse_job_list)


        ###--------------------------------------------兼职  filter=free&
        category = u'活动策划'
        job_type = 1
        url = 'http://'+city_code+'.58.com/event/?PGTID=14447886322250.5926351698581129&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'网络营销'
        job_type = 1
        url = 'http://'+city_code+'.58.com/wangluoyingxiao/?PGTID=135679506189371356626915410&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'SEO优化'
        job_type = 1
        url = 'http://'+city_code+'.58.com/partimeseo/?PGTID=152025317189371357186889848&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'美工设计'
        job_type = 1
        url = 'http://'+city_code+'.58.com/computer/?PGTID=144435722189371389986264904&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'图片处理'
        job_type = 1
        url = 'http://'+city_code+'.58.com/imageps/?PGTID=171439643189371400305490092&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'礼仪模特'
        job_type = 1
        url = 'http://'+city_code+'.58.com/liyiyanyi/?PGTID=198039147189371407914588601&ClickID=1'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'客服'
        job_type = 1
        url = 'http://'+city_code+'.58.com/jianzhi/?key=%E5%AE%A2%E6%9C%8D&cmcskey=%E5%AE%A2%E6%9C%8D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'美工'
        job_type = 1
        url = 'http://'+city_code+'.58.com/computer/?key=%E7%BE%8E%E5%B7%A5&cmcskey=%E7%BE%8E%E5%B7%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'运营'
        job_type = 1
        url = 'http://'+city_code+'.58.com/jianzhi/?key=%E8%BF%90%E8%90%A5&cmcskey=%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'推广'
        job_type = 1
        url ='http://'+city_code+'.58.com/jianzhi/?key=%E6%8E%A8%E5%B9%BF&cmcskey=%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)
        category = u'摄影师'
        job_type = 1
        url ='http://'+city_code+'.58.com/jianzhi/?key=%E6%91%84%E5%BD%B1%E5%B8%88&cmcskey=%E6%91%84%E5%BD%B1%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url,meta={"category": category, "city": city,'job_type':job_type} ,callback=self.parse_job_list)

        #  12.13 add 全职
        category = u'CRM'
        url = 'http://'+city_code+'.58.com/job/?key=crm&cmcskey=crm&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'客户关系管理'
        url = 'http://'+city_code+'.58.com/job/?key=%252525E5%252525AE%252525A2%252525E6%25252588%252525B7%252525E5%25252585%252525B3%252525E7%252525B3%252525BB%252525E7%252525AE%252525A1%252525E7%25252590%25252586&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'审单'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%AE%A1%E5%8D%95&cmcskey=%E5%AE%A1%E5%8D%95&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'订单专员'
        url = 'http://'+city_code+'.58.com/job/?key=%E8%AE%A2%E5%8D%95%E4%B8%93%E5%91%98&cmcskey=%E8%AE%A2%E5%8D%95%E4%B8%93%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'打包'
        url = 'http://'+city_code+'.58.com/job/?key=%E6%89%93%E5%8C%85&cmcskey=%E6%89%93%E5%8C%85&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'UI设计'
        url = 'http://'+city_code+'.58.com/zpguanggao/?key=ui%E8%AE%BE%E8%AE%A1&cmcskey=ui%E8%AE%BE%E8%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=1_4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'化妆师'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%8C%96%E5%A6%86%E5%B8%88&cmcskey=%E5%8C%96%E5%A6%86%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'彩妆师'
        url = 'http://'+city_code+'.58.com/meirongjianshen/?key=%E5%BD%A9%E5%A6%86%E5%B8%88&cmcskey=%E5%BD%A9%E5%A6%86%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'店长'
        url = 'http://'+city_code+'.58.com/zptaobao/?PGTID=0d202408-0004-f156-cab1-579c6463c983&ClickID=2&key=%252525E5%252525BA%25252597%252525E9%25252595%252525BF&filk=true'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'品牌经理'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%93%81%E7%89%8C%E7%BB%8F%E7%90%86&cmcskey=%E5%93%81%E7%89%8C%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'采购经理'
        url = 'http://'+city_code+'.58.com/job/?key=%E9%87%87%E8%B4%AD%E7%BB%8F%E7%90%86&cmcskey=%E9%87%87%E8%B4%AD%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'淘宝运营'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%E6%B7%98%E5%AE%9D%E8%BF%90%E8%90%A5&filk=true&PGTID=0d309702-0004-f0e7-45db-787401297648&ClickID=1'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'运营'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%252525E8%252525BF%25252590%252525E8%25252590%252525A5&filk=true&PGTID=0d309702-0004-f6a6-2c62-5440388d01ce&ClickID=1'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'客服'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%252525E5%252525AE%252525A2%252525E6%2525259C%2525258D&filk=true&PGTID=0d309702-0004-f6a6-2c62-5440388d01ce&ClickID=1'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'美工'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%252525E7%252525BE%2525258E%252525E5%252525B7%252525A5&filk=true&PGTID=0d309702-0004-f6a6-2c62-5440388d01ce&ClickID=1'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'店长'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%252525E5%252525BA%25252597%252525E9%25252595%252525BF&filk=true&PGTID=0d309702-0004-f6a6-2c62-5440388d01ce&ClickID=1'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'天猫运营'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%A4%A9%E7%8C%AB%E8%BF%90%E8%90%A5&cmcskey=%E5%A4%A9%E7%8C%AB%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'京东运营'
        url = 'http://'+city_code+'.58.com/job/?key=%E4%BA%AC%E4%B8%9C%E8%BF%90%E8%90%A5&cmcskey=%E4%BA%AC%E4%B8%9C%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'跨境运营'
        url = 'http://'+city_code+'.58.com/job/?key=%E8%B7%A8%E5%A2%83%E8%BF%90%E8%90%A5&cmcskey=%E8%B7%A8%E5%A2%83%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'微信运营'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5&cmcskey=%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'微博运营'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%BE%AE%E5%8D%9A%E8%BF%90%E8%90%A5&cmcskey=%E5%BE%AE%E5%8D%9A%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'店长助理'
        url = 'http://'+city_code+'.58.com/chaoshishangye/?key=%E5%BA%97%E9%95%BF%E5%8A%A9%E7%90%86&cmcskey=%E5%BA%97%E9%95%BF%E5%8A%A9%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网站编辑'
        url = 'http://'+city_code+'.58.com/job/?key=%252525E7%252525BD%25252591%252525E7%252525AB%25252599%252525E7%252525BC%25252596%252525E8%252525BE%25252591&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网络编辑'
        url = 'http://'+city_code+'.58.com/tech/?key=%E7%BD%91%E7%BB%9C%E7%BC%96%E8%BE%91&cmcskey=%E7%BD%91%E7%BB%9C%E7%BC%96%E8%BE%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'活动策划'
        url = 'http://'+city_code+'.58.com/zptaobao/?key=%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92&cmcskey=%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'品牌策划'
        url = 'http://'+city_code+'.58.com/shichang/?key=%E5%93%81%E7%89%8C%E7%AD%96%E5%88%92&cmcskey=%E5%93%81%E7%89%8C%E7%AD%96%E5%88%92&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'品牌营销'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%93%81%E7%89%8C%E8%90%A5%E9%94%80&cmcskey=%E5%93%81%E7%89%8C%E8%90%A5%E9%94%80&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'采购'
        url = 'http://'+city_code+'.58.com/zpshangwumaoyi/?key=%E9%87%87%E8%B4%AD&cmcskey=%E9%87%87%E8%B4%AD&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'新媒体运营'
        url = 'http://'+city_code+'.58.com/tech/?key=%E6%96%B0%E5%AA%92%E4%BD%93%E8%BF%90%E8%90%A5&cmcskey=%E6%96%B0%E5%AA%92%E4%BD%93%E8%BF%90%E8%90%A5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'销售总监'
        url = 'http://'+city_code+'.58.com/xiaoshouzongjian/?key=%E9%94%80%E5%94%AE%E6%80%BB%E7%9B%91&cmcskey=%E9%94%80%E5%94%AE&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'市场总监'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%B8%82%E5%9C%BA%E6%80%BB%E7%9B%91&cmcskey=%E5%B8%82%E5%9C%BA%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网店推广'
        url = 'http://'+city_code+'.58.com/job/?key=%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF&cmcskey=%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网络推广'
        url = 'http://'+city_code+'.58.com/tech/?key=%E7%BD%91%E7%BB%9C%E6%8E%A8%E5%B9%BF&cmcskey=%E7%BD%91%E7%BB%9C%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网站推广'
        url = 'http://'+city_code+'.58.com/tech/?key=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&cmcskey=%E7%BD%91%E7%AB%99%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'SEM'
        url = 'http://'+city_code+'.58.com/job/?key=sem&cmcskey=sem&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'数据分析'
        url = 'http://'+city_code+'.58.com/zpcaiwushenji/?key=%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90&cmcskey=%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'销售经理'
        url = 'http://'+city_code+'.58.com/xiaoshoujingli/?key=%E9%94%80%E5%94%AE%E7%BB%8F%E7%90%86&cmcskey=%E9%94%80%E5%94%AE%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'客户经理'
        url = 'http://'+city_code+'.58.com/kehujingli/?key=%E5%AE%A2%E6%88%B7%E7%BB%8F%E7%90%86&cmcskey=%E5%AE%A2%E6%88%B7%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'电话销售'
        url = 'http://'+city_code+'.58.com/dianhuaxiaoshou/?key=%E7%94%B5%E8%AF%9D%E9%94%80%E5%94%AE&cmcskey=%E7%94%B5%E8%AF%9D&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'BD经理'
        url = 'http://'+city_code+'.58.com/job/?key=bd%E7%BB%8F%E7%90%86&cmcskey=bd%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'市场推广'
        url = 'http://'+city_code+'.58.com/shichang/?key=%E5%B8%82%E5%9C%BA%E6%8E%A8%E5%B9%BF&cmcskey=%E5%B8%82%E5%9C%BA%E6%8E%A8%E5%B9%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'市场营销'
        url = 'http://'+city_code+'.58.com/shichang/?key=%E5%B8%82%E5%9C%BA%E8%90%A5%E9%94%80&cmcskey=%E5%B8%82%E5%9C%BA%E8%90%A5%E9%94%80&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'市场策划'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%B8%82%E5%9C%BA%E7%AD%96%E5%88%92&cmcskey=%E5%B8%82%E5%9C%BA%E7%AD%96%E5%88%92&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'行政经理'
        url = 'http://'+city_code+'.58.com/renli/?key=%E8%A1%8C%E6%94%BF%E7%BB%8F%E7%90%86&cmcskey=%E8%A1%8C%E6%94%BF%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'总经理'
        url = 'http://'+city_code+'.58.com/job/?key=%E6%80%BB%E7%BB%8F%E7%90%86&cmcskey=%E6%80%BB%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'行政总监'
        url = 'http://'+city_code+'.58.com/renli/?key=%E8%A1%8C%E6%94%BF%E6%80%BB%E7%9B%91&cmcskey=%E8%A1%8C%E6%94%BF%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人事总监'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E4%BA%8B%E6%80%BB%E7%9B%91&cmcskey=%E4%BA%BA%E4%BA%8B%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人力资源总监'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E5%8A%9B%E8%B5%84%E6%BA%90%E6%80%BB%E7%9B%91&cmcskey=%E4%BA%BA%E5%8A%9B%E8%B5%84%E6%BA%90%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'财务总监'
        url = 'http://'+city_code+'.58.com/job/?key=%E8%B4%A2%E5%8A%A1%E6%80%BB%E7%9B%91&cmcskey=%E8%B4%A2%E5%8A%A1%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'前台'
        url = 'http://'+city_code+'.58.com/renli/?key=%E5%89%8D%E5%8F%B0&cmcskey=%E5%89%8D%E5%8F%B0&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'行政助理'
        url = 'http://'+city_code+'.58.com/renli/?key=%E8%A1%8C%E6%94%BF%E5%8A%A9%E7%90%86&cmcskey=%E8%A1%8C%E6%94%BF%E5%8A%A9%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'文秘'
        url = 'http://'+city_code+'.58.com/renli/?key=%E6%96%87%E7%A7%98&cmcskey=%E6%96%87%E7%A7%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'秘书'
        url = 'http://'+city_code+'.58.com/renli/?key=%E7%A7%98%E4%B9%A6&cmcskey=%E7%A7%98%E4%B9%A6&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人事行政'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E4%BA%8B%E8%A1%8C%E6%94%BF&cmcskey=%E4%BA%BA%E4%BA%8B%E8%A1%8C%E6%94%BF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人事专员'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E4%BA%8B%E4%B8%93%E5%91%98&cmcskey=%E4%BA%BA%E4%BA%8B%E4%B8%93%E5%91%98&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人事经理'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E4%BA%8B%E7%BB%8F%E7%90%86&cmcskey=%E4%BA%BA%E4%BA%8B%E7%BB%8F%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'人事主管'
        url = 'http://'+city_code+'.58.com/renli/?key=%E4%BA%BA%E4%BA%8B%E4%B8%BB%E7%AE%A1&cmcskey=%E4%BA%BA%E4%BA%8B%E4%B8%BB%E7%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'出纳'
        url = 'http://'+city_code+'.58.com/job/?key=%252525E5%25252587%252525BA%252525E7%252525BA%252525B3&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'会计'
        url = 'http://'+city_code+'.58.com/job/?key=%E4%BC%9A%E8%AE%A1&cmcskey=%E4%BC%9A%E8%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'审计'
        url = 'http://'+city_code+'.58.com/zpcaiwushenji/?key=%E5%AE%A1%E8%AE%A1&cmcskey=%E5%AE%A1%E8%AE%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'法务'
        url = 'http://'+city_code+'.58.com/job/?key=%E6%B3%95%E5%8A%A1&cmcskey=%E6%B3%95%E5%8A%A1&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'产品经理'
        url = 'http://'+city_code+'.58.com/job/?key=%252525E4%252525BA%252525A7%252525E5%25252593%25252581%252525E7%252525BB%2525258F%252525E7%25252590%25252586&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'产品总监'
        url = 'http://'+city_code+'.58.com/zpguanli/?key=%E4%BA%A7%E5%93%81%E6%80%BB%E7%9B%91&cmcskey=%E4%BA%A7%E5%93%81%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'架构师'
        url = 'http://'+city_code+'.58.com/tech/?key=%E6%9E%B6%E6%9E%84%E5%B8%88&cmcskey=%E6%9E%B6%E6%9E%84%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'技术总监'
        url = 'http://'+city_code+'.58.com/job/?key=%E6%8A%80%E6%9C%AF%E6%80%BB%E7%9B%91&cmcskey=%E6%8A%80%E6%9C%AF%E6%80%BB%E7%9B%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'产品助理'
        url = 'http://'+city_code+'.58.com/tech/?key=%E4%BA%A7%E5%93%81%E5%8A%A9%E7%90%86&cmcskey=%E4%BA%A7%E5%93%81%E5%8A%A9%E7%90%86&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=1_4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'web前端'
        url = 'http://'+city_code+'.58.com/job/?key=web%E5%89%8D%E7%AB%AF&cmcskey=web%E5%89%8D%E7%AB%AF&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'前端开发'
        url = 'http://'+city_code+'.58.com/job/?key=%E5%89%8D%E7%AB%AF%E5%BC%80%E5%8F%91&cmcskey=%E5%89%8D%E7%AB%AF%E5%BC%80%E5%8F%91&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'html5'
        url = 'http://'+city_code+'.58.com/job/?key=html5&cmcskey=html5&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'ios'
        url = 'http://'+city_code+'.58.com/job/?key=ios&cmcskey=ios&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'Android'
        url = 'http://'+city_code+'.58.com/job/?key=android&cmcskey=android&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'java'
        url = 'http://'+city_code+'.58.com/tech/?key=java&cmcskey=java&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'PHP'
        url = 'http://'+city_code+'.58.com/tech/?key=php&cmcskey=php&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'C++'
        url = 'http://'+city_code+'.58.com/tech/?key=c%2B%2B&cmcskey=c%2B%2B&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'软件测试'
        url = 'http://'+city_code+'.58.com/job/?key=%E8%BD%AF%E4%BB%B6%E6%B5%8B%E8%AF%95&cmcskey=%E8%BD%AF%E4%BB%B6%E6%B5%8B%E8%AF%95&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'测试工程师'
        url = 'http://'+city_code+'.58.com/job/?key=%E6%B5%8B%E8%AF%95%E5%B7%A5%E7%A8%8B%E5%B8%88&cmcskey=%E6%B5%8B%E8%AF%95%E5%B7%A5%E7%A8%8B%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'网络工程师'
        url = 'http://'+city_code+'.58.com/tech/?key=%E7%BD%91%E7%BB%9C%E5%B7%A5%E7%A8%8B%E5%B8%88&cmcskey=%E7%BD%91%E7%BB%9C%E5%B7%A5%E7%A8%8B%E5%B8%88&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)
        category = u'运维'
        url = 'http://'+city_code+'.58.com/job/?key=%E8%BF%90%E7%BB%B4&cmcskey=%E8%BF%90%E7%BB%B4&final=1&jump=2&specialtype=gls&canclequery=isbiz%3D0&sourcetype=4'
        yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_job_list)

        #测试-------
        #url= 'http://hz.58.com/zpwuliucangchu/23642787070089x.shtml'
        #yield Request(url,meta={"category": u'客服', "city": city,'job_type':1} ,callback=self.parse_job_detail)

    def parse_job_list(self, response):
        if  'job_type' in response.meta:
            job_type = response.meta['job_type']
        else:
            job_type = 3 #默认为全职
        city = response.meta["city"]
        category = response.meta["category"]
        job_list = response.xpath('//*[@id="infolist"]/dl')  # 职位列表
        log.msg('category: '+str(category)+" "+response.url)

        if len(job_list) > 0:
            dd_yester = datetime.datetime.utcnow() + datetime.timedelta(hours=8) + datetime.timedelta(days=-1)
            dd_yester = datetime.datetime.strptime(str(dd_yester.year)+'-'+str(dd_yester.month)+'-'+str(dd_yester.day),'%Y-%m-%d')
            for selector in job_list:
                refresh_date = selector.xpath('dd/text()')[-1].extract().strip()  # 更新时间
                date = re.findall(self.date_matcher, refresh_date)  
                
                if u'天前' in refresh_date:
                    log.msg('stop crawl, refresh date:' + refresh_date)
                    return
                else:
                    if date:
                        com_refre_date = datetime.datetime.strptime(str(dd_yester.year)+'-'+date[0],'%Y-%m-%d')
                        ss_date = com_refre_date - dd_yester

                        if not ( ss_date.total_seconds() <= 86400 if ss_date.total_seconds()>0 else ss_date.total_seconds() >= -86400):
                            log.msg('stop crawl, refresh date:' + refresh_date)
                            return

                    tmp = selector.xpath('dt/a/@href').extract()
                    if tmp:
                        url = selector.xpath('dt/a/@href').extract()[0]
                        if 1 == job_type:
                            yield Request(url=url, meta={"category": category, "city": city,'job_type':job_type}, callback=self.parse_job_jianzhi_detail)
                        else:
                            yield Request(url=url, meta={"category": category, "city": city,'job_type':job_type}, callback=self.parse_job_detail)

            # 分页
            next_page = response.xpath('//div[@class="pager"]/a[@class="next"]/@href').extract()  # 判断下一页链接是否存在
            refresh_date = job_list[-1].xpath('dd/text()')[-1].extract().strip()  # 更新时间
            log.msg('refresh_date:' + refresh_date)
            if next_page and refresh_date:
                date = re.findall(self.date_matcher, refresh_date)
                if date or u'天前' in refresh_date:  # 如果以日期格式显示，说明已经不是最新的，不再继续抓取
                    log.msg("stop crawl, refresh date:" + date[0])
                    return
                else:
                    domain = re.findall(self.domain_matcher, response.url)[0]
                    url = domain + next_page[0]
                    log.msg("nextpage:" + next_page[0])
                    yield Request(url=url, meta={'city': city, 'category': category,'job_type':job_type}, callback=self.parse_job_list)



    def parse_job_detail(self, response):
        
        item = JobItem()
        item['prop'] = job_type_dict.get(response.meta["job_type"],u'全职')
        item["city"] = response.meta["city"]
        item["category"] = response.meta["category"]
        item["url"] = response.url
        item["name"] = response.xpath('//div[@class="headConLeft"]/h1/text()').extract()
        addre1 = response.xpath('//div[@class="xq"]/ul/li[3]/span[2]/text()').extract()
        addree = addre1[0]+'@@@'
        addres = response.xpath('//div[@class="xq"]/ul/li[3]/span[3]/a')
        for addre in addres:
            addre2 = addre.xpath('text()').extract()
            addree += addre2[0]
        item['address'] = addree
        item["salary"] = response.xpath('//div[@class="xq"]/ul/li[1]/div[1]/span[2]/strong/text()').extract()
        item["experience"] = response.xpath('//div[@class="xq"]/ul/li[2]/div[2]/text()').extract()
        item["education"] = response.xpath('//div[@class="xq"]/ul/li[1]/div[2]/text()').extract()
        welfarelabels = response.xpath('//*[@id="infoview"]/ul[@class="cbright"]/li/div/span/text()').extract()
        item["welfarelabel"] = ",".join(map(lambda x: x.strip(), welfarelabels))[:120]
        refresh_number = response.xpath('//ul[@class="headTag"]/li[1]/span/strong/text()').extract()
        refresh_unit = response.xpath('//ul[@class="headTag"]/li[1]/span/text()').extract()
        item["publish"] = refresh_number[0].strip() + refresh_unit[1].strip() if refresh_unit else ''

        contact_code = response.xpath('//input[@id="pagenum"]/@value').extract()
        if contact_code and contact_code[0].strip():
            contact_code[0] = contact_code[0].strip()
            item["contact"] = 'http://image.58.com/showphone.aspx?t=v55&v=' + contact_code[0].split('_')[0]  # 如果有两个电话用下划线分割，我们只去第一个
        else:
            log.msg('quanzhi_ no contact found: ' + response.url)
            return
        item["contact_name"] = self.name_matcher.findall(response.body)
        # http://image.58.com/showphone.aspx?t=v55&v=F58D0054214E9FE70ECBCE1BEA839C2F6
        # http://image.58.com/showphone.aspx?t=v55&v=6E71DF44A6E3245B04E6E3AE268573395
        # desc = response.xpath('//div[@id="zhiwei"]/div[@class="posMsg borb"]/p/span/text()').extract()
        # item["desc"] = "\n".join(filter(lambda x: x, map(lambda x: x.strip(), desc)))
        item["desc"] = response.xpath('//div[@id="zhiwei"]/div[@class="posMsg borb"]').extract()
        item["company"] = response.xpath('//a[@class="companyName"]/text() | //div[@class="compTitle"]/a/text()').extract()
        item["industry"] = response.xpath('//div[@class="compMsg clearfix"]/ul/li[1]/a/text()').extract()
        item["scale"] = response.xpath('//div[@class="compMsg clearfix"]/ul/li[@class="scale"]/text()').extract()
        item["comprop"] = response.xpath('//div[@class="compMsg clearfix"]/ul/li[2]/text()').extract()
        item['shopaddr'] = ''
        comprofile = response.xpath('//div[@id="gongsi"]/p[1]/text()').extract()    # 公司介绍，先尝试抓取P里面的文字，如果没有，则抓取p/span中的文字
        comprofile = filter(lambda x: x, map(lambda x: x.strip(), comprofile))
        if not comprofile:
            comprofile = response.xpath('//div[@id="gongsi"]/p[1]/span/text()').extract()
            comprofile = filter(lambda x: x, map(lambda x: x.strip(), comprofile))
        item["comprofile"] = '\n'.join(comprofile)
        
        shop_detail = response.xpath('//div[@class="companyCon mod detailRightAd"]/a/@href').extract()
        if shop_detail:
            yield Request(shop_detail[0],meta={'item':item},callback=self.parse_shop_url)

        else:
            log.msg('ERROR:No found shop url!')
            item['shopurl'] = ''
            yield item

    def parse_shop_url(self,response):
        item = response.meta['item']
        tmp = u'网址'
        shopurl = response.xpath('//div[@class="basicMsg"]/table//th[contains(text(),"%s")]/following-sibling::*[1]//a/@href'%tmp).extract()
        #log.msg('@#'*30+str(shopurl))
        if shopurl and u'.58.com'not in shopurl[0] :
            item['shopurl'] = shopurl[0].strip()
        else:
            item['shopurl'] = ''

        if 'job_type' in response.meta and 1 == response.meta['job_type']:
            tmp = u'性质'
            comprop = response.xpath('//div[@class="basicMsg"]/table//th[contains(text(),"%s")]/following-sibling::*[1]/text()'%tmp).extract()
            if comprop:
                item["comprop"] = comprop[0].strip()
            tmp = u'规模'
            scale = response.xpath('//div[@class="basicMsg"]/table//th[contains(text(),"%s")]/following-sibling::*[1]/text()'%tmp).extract()
            if scale:
                item["scale"] = scale[0].strip()
            tmp = u'行业'
            industry = response.xpath('//div[@class="basicMsg"]/table//th[contains(text(),"%s")]/following-sibling::*[1]/span/a/text()'%tmp).extract()
            if industry:
                item["industry"] = ' '.join(industry)

            item['shopaddr'] = pick_xpath(response,'//td[@class="adress"]/span/text()')
        yield item

        ####所有extract结果都在pipeline内部进行取0位元素操作

    def  parse_job_jianzhi_detail(self,response):
        item = JobItem()
        sel = Selector(response)
        item['prop'] = job_type_dict.get(response.meta["job_type"],u'全职')
        item["city"] = response.meta["city"]
        item["category"] = response.meta["category"]
        item["url"] = response.url
        item["name"] = pick_xpath(sel,'//*[@id="main"]/div[@class="leftbar pad60"]/h1/text()')

        item["address"] = pick_xpaths(sel,'//div[@class="xq"]//dl[last()]/dd/a/text()','，')
        item["salary"] = pick_xpath(sel,'//div[@class="xq"]//dl[4]/dd/text()').replace('\n','')
        item["experience"] = ''
        item["education"] = ''
        item["welfarelabel"] = ''
        item["publish"] = pick_xpath(sel,'//*[@id="main"]//span[@class="timeD"]/text()')

        contact_code = pick_xpath(sel,'//input[@id="pagenum"]/@value')
        if contact_code :
            item["contact"] = 'http://image.58.com/showphone.aspx?t=v55&v=' + contact_code[0].split('_')[0]  # 如果有两个电话用下划线分割，我们只去第一个
        else:
            log.msg('jianzhi_ no contact found: ' + response.url)
            return
        item["contact_name"] = self.name_matcher.findall(response.body)

        item["desc"] = pick_xpaths(sel,'//*[@id="zhiwei"]/dl/dd//span/text()','\n')+ '\n' + item["salary"]
        item["salary"] = u'面议'
        item['company'] = pick_xpath(sel,'//*[@class="xq"]//div[@class="compMsg"]/a[1]/text()')
        shop_detail = pick_xpath(sel,'//*[@class="xq"]//div[@class="compMsg"]/a[1]/@href')

        item["industry"] = ''
        item["scale"] = ''
        item["comprop"] = ''
        item["comprofile"] = pick_xpaths(sel,'//div[@id="gongsi"]/p[1]/text()','\n')   # 公司介绍，先尝试抓取P里面的文字，如果没有，则抓取p/span中的文字
        item['shopaddr'] = ''

        if shop_detail:
            yield Request(shop_detail,meta={'item':item,'job_type':response.meta["job_type"]},callback=self.parse_shop_url)
        else:
            log.msg('ERROR:No found shop url!')
            item['shopurl'] = ''
            yield item
