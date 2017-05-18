#-*-coding:utf-8 -*-
from scrapy.spider import Spider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.xlib.pydispatch import dispatcher
from scrapy.core.downloader import Downloader, Slot
from scrapy.item import Item, Field
from scrapy import signals
from scrapy import log
import re
import os
import datetime
import time
import json
import sys

class PositionItem(Item):
    category   = Field()  # 职位类别
    name       = Field()  # 职位名称
    address    = Field()  # 工作地址
    salary     = Field()  # 薪资
    experience = Field()  # 工作经验
    education  = Field()  # 学历要求
    prop       = Field()  # 职位性质
    welfare    = Field()  # 公司福利
    publish    = Field()  # 发布时间或更新时间
    url        = Field()  # 原始url
    desc       = Field()  # 职位描述
    company    = Field()  # 公司名称
    industry   = Field()  # 行业
    scale      = Field()  # 公司规模
    shopaddr   = Field()  # 店铺详细地址
    comprop    = Field()  # 公司性质
    comprofile = Field()  # 公司简介
    origin     = Field()  # 来源网站


def pick_str(str, bs, es='"'):
    bslen = len(bs)
    a = str.find(bs)
    if a != -1:
        b = str.find(es, a+bslen)
        if b != -1:
            return str[a+bslen:b].strip()
    return ''

def pick_xpath(sel, path):
    value = sel.xpath(path).extract()
    if value and value[0].strip():
        return value[0].strip()
    return ''

def fmt_text(str, level=2):
    if not str:
        return ''

    #str = str.strip().replace(',','%2C').replace('\n', '\\n')
    str = str.strip().replace('\n', ' ')
    if level == 2:
        str = re.sub(r'/$', '', re.sub(r'&#\d+;?','',str.replace('&amp;','&').replace('&amp;','&')).strip())
    return str

class ProductSpider(Spider):
    name = "jobzl"
    download_delay = 1
    start_urls = ['http://www.zhaopin.com']

    last_ndays = [u'\u4eca\u5929', u'\u6628\u5929', u'\u524d\u5929'] # today,yesterday,the day before yesterday
    para = ''

    def __init__(self, para=None, *args, **kwargs):
        super(ProductSpider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)

        for d in range(0,1):
            dd = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=d)
            ndays_ago = "%02d-%02d" % (dd.month, dd.day)
            self.last_ndays.append(ndays_ago)

        if (para is not None):
            self.para = para

    def engine_opened(self):
        log.msg('para:' + self.para)
        self.crawler.engine.downloader.slots['sou.zhaopin.com'] = Slot(5, 5, self.settings)

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
        if not self.para:
            raise Exception('Not have para!')

        for kw in self.para.split(','):
            url = "http://sou.zhaopin.com/jobs/searchresult.ashx?jl=选择地区&kw=%s&p=1&kt=3&isadv=0" % (kw)
            #url = "http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E9%80%89%E6%8B%A9%E5%9C%B0%E5%8C%BA&kw="+kw+"&p=1&kt=3&isadv=0"
            yield Request(url, dont_filter=True, callback=self.parse_item_s)

    def parse_item_s(self, response):
        sel = Selector(response)
        log.msg('-'*30+response.url) 
        dd = ''
        tables = sel.xpath('//*[@id="newlist_list_content_table"]/table')
        for t in tables[1:]:
            item = PositionItem()

            name = t.xpath('.//td[@class="zwmc"]/div/a//text()').extract()
            item['name'] =  ''.join(name)
            item['address'] = pick_xpath(t, './/td[@class="gzdd"]/text()')
            item['salary'] = pick_xpath(t, './/td[@class="zwyx"]/text()')
            item['publish'] = pick_xpath(t, './/td[@class="gxsj"]/span/text()')
            item['url'] = pick_xpath(t, './/td[@class="zwmc"]/div/a[1]/@href')
            item['company'] = pick_xpath(t, './/td[@class="gsmc"]/a/text()')

            dd = item['publish']

            url = pick_xpath(t, './/td[@class="zwmc"]/div/a[1]/@href')
            if url:
                yield Request(url, meta={'item':item}, callback=self.parse_item)

            if 'desc' in item:
                yield item

        # next page
        if dd in self.last_ndays:
            np = pick_xpath(sel, '//*[@class="pagesDown-pos"]/a/@href')
            if np:
                yield Request(np, dont_filter=True, callback=self.parse_item_s)

    def parse_item(self, response):
        item = response.meta['item']

        sel = Selector(response)

        item['welfare'] = ' '.join(sel.xpath('/html/body/div[3]/div[1]/div[1]/div/span/text()').extract())
        lis=sel.xpath('//*[@class="terminal-ul clearfix"]/li')
        lis=sel.xpath('//*[@class="terminal-ul clearfix"]/li')
        for l in lis:
            key = pick_xpath(l, './span/text()')
            val = pick_xpath(l, './strong/text()')
            if key == u'\u5de5\u4f5c\u6027\u8d28\uff1a':
                item['prop'] = val
            
            elif key == u'\u5de5\u4f5c\u7ecf\u9a8c\uff1a':
                item['experience'] = val
            
            elif key == u'\u6700\u4f4e\u5b66\u5386\uff1a':
                item['education'] = val
            
            elif key == u'\u804c\u4f4d\u7c7b\u522b\uff1a':
                item['category'] = pick_xpath(l, './strong/a[2]/text()')

        infos = sel.xpath('//*[@class="terminal-ul clearfix terminal-company mt20"]/li')
        for i in infos:
            key = pick_xpath(i, './span/text()')
            val = pick_xpath(i, './strong/text()')
            if key == u'\u516c\u53f8\u884c\u4e1a\uff1a':
                item['industry'] = pick_xpath(i, './strong/a/text()')
            
            elif key == u'\u516c\u53f8\u89c4\u6a21\uff1a':
                item['scale'] = val

            elif key == u'\u516c\u53f8\u5730\u5740\uff1a':
                item['shopaddr'] = val

            elif key == u'\u516c\u53f8\u6027\u8d28\uff1a':
                item['comprop'] = val
        
        main = sel.xpath('//div[@class="tab-inner-cont"]')

        # desc
        d1 = '\n'.join([fmt_text(x) for x in main[0].xpath('.//p/text()').extract()])
        d2 = '\n'.join([fmt_text(x) for x in main[0].xpath('.//span/text()').extract()])
        item['desc'] = "%s%s\n%s\n%s" % (d1, d2, pick_xpath(main[0],'.//h1/text()'), pick_xpath(main[0],'.//h2/text()'))

        # comprofile
        #item['comprofile'] = '\n'.join([fmt_text(x) for x in main[1].xpath('.//span/text()').extract()])
        item['comprofile'] = '\n'.join([fmt_text(x) for x in main[1].xpath('.//p//text()|.//div//text()').extract()])
        if 'desc' not in item:
            item['desc'] = 'description'
        item['origin'] = 'zhilian'
        return item


#http://jobs.zhaopin.com/613909023250039.htm
#http://jobs.zhaopin.com/156328115267193.htm
#http://jobs.zhaopin.com/202586113250128.htm
#http://jobs.zhaopin.com/606880629250114.htm?ssidkey=y&ss=409&ff=03
#公司简介----已知的四个版本


