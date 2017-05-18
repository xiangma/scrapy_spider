# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from datetime import datetime, timedelta
from maijiajob.items import JobItem
from scrapy import log
import time
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import os

class TaojobSpider(scrapy.Spider):
    name = "taojob"
    download_delay = 3
    start_urls = (
        'http://zhaopin.alibado.com/index.htm',
    )
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() + timedelta(hours=8) - timedelta(hours=24)).strftime("%Y-%m-%d")
    domain = 'http://zhaopin.alibado.com'
    url_tmp = u"http://zhaopin.alibado.com/job/mainSearch.htm?searchType=1&salary=0&scale=" \
              u"0&province=&city=&zone=&keyword={keyword}&shopLevel=0&workMode=2&workMode=3&t={time}#J_Search"
    key_words = [u'淘宝客服', u'天猫客服', u'网店客服', u'电商客服', u'售前客服', u'售后客服', u'客服主管', u'客服经理', u'客服专员',
                 u'淘宝美工', u'天猫美工', u'网店美工', u'平面设计', u'网店设计', u'淘宝推广', u'天猫推广', u'网店推广']
    para = ''

    def __init__(self, para=None, *args, **kwargs):
        super(TaojobSpider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)
        if para:
            self.para = para

    def engine_opened(self):
        log.msg('para:' + self.para)

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
        """点击高级搜索，进入搜索页"""
        url = response.xpath('//a[@class="job-header-super-search"]/@href').extract()[0]
        yield Request(url= 'http:' + url, callback=self.parse_search)

    def parse_search(self, response):
        """搜索页，拼搜索url"""
        for key_word in self.key_words:
            url = self.url_tmp.format(keyword=key_word, time=time.time())
            yield Request(url=url, meta={"keyword": key_word}, callback=self.parse_list)

    def parse_list(self, response):
        keyword = response.meta["keyword"]
        urls = response.xpath('//table[@class="job-table"]/tr/td[1]/div/a/@href').extract()
        for path in urls:
            url = self.domain + path.strip()
            yield Request(url=url, callback=self.parse_job)

        # 分页，抓今天更新的职位和昨天更新的职位
        current_date = response.xpath('//table[@class="job-table"]/tr[last()]/td[last()]/text()').extract()[0]
        log.msg('current_date:' + current_date)
        next_page = response.xpath('//ul[@class="x-pager-list"]/li[last()]/a/@href').extract()[0]
        if "javascript" not in next_page and (current_date == self.today or current_date == self.yesterday):
            log.msg("request next page for keyword:" + keyword + ", refresh date:" + current_date)
            url = 'http://zhaopin.alibado.com/job/mainSearch.htm' + next_page
            yield Request(url=url, meta={'keyword': keyword}, callback=self.parse_list)
        else:
            log.msg('stop crawl keyword:' + keyword)

    def parse_job(self, response):
        if 'xue.alibado.com/user/signin.htm' in response.url:
            log.msg("oops ! baned !")  # 重定向到了登陆页
            return

        item = JobItem()
        item["url"] = response.url
        item['name'] = response.xpath('//div[@class="detail-page"]/h3[1]/text()').extract()[0].split(u'：')
        item['prop'] = response.xpath('//div[@class="detail-page"]/ul/li[1]/text()').extract()[0].split(u'：')[1].strip()
        item["address"] = response.xpath('//div[@class="detail-page"]/ul/li[2]/text()').extract()[0].split(u'：')[1].strip()  # 含有很多空白字符，需要后续处理
        item["salary"] = response.xpath('//div[@class="detail-page"]/ul/li[3]/text()').extract()[0].split(u'：')[1].strip()
        item['category'] = response.xpath('//div[@class="detail-page"]/ul/li[4]/text()').extract()[0].split(u'：')[1].strip()
        item["education"] = response.xpath('//div[@class="detail-page"]/ul/li[5]/text()').extract()[0].split(u'：')[1].strip()
        item["experience"] = response.xpath('//div[@class="detail-page"]/ul/li[6]/text()').extract()[0].split(u'：')[1].strip()
        item["gender"] = response.xpath('//div[@class="detail-page"]/ul/li[7]/text()').extract()[0].split(u'：')[1].strip()
        item["publish"] = response.xpath('//div[@class="detail-page"]/ul/li[9]/text()').extract()[0].split(u'：')[1].strip()
        # 职位描述
        desc = response.xpath('//div[@class="detail-page"]/p[1]/text()').extract()
        desc = map(lambda x: x.strip(), desc)  # 去掉每一小段文字的空白字符
        desc = "\n".join(desc)
        # 任职要求
        require = response.xpath('//div[@class="detail-page"]/p[1]/text()').extract()
        require = map(lambda x: x.strip(), require)
        require = "\n".join(require)
        item["desc"] = desc + "\n" + require
        item["shop"] = response.xpath('//div[@class="face-detail"]/h3/a/text()').extract()  # 店铺名称
        item["shopurl"] = response.xpath('//div[@class="face-detail"]/h3/a/@href').extract()  # 店铺名称
        item["company"] = response.xpath('//div[@class="face-detail"]/h4/a/text()').extract()  # 公司名称
        item["industry"] = response.xpath('//div[@class="info-list"]/div[@class="info-item"][1]/text()').extract()  # 行业
        item["scale"] = response.xpath('//div[@class="info-list"]/div[@class="info-item"][2]/text()').extract()  # 公司规模
        item["shopaddr"] = response.xpath('//div[@class="info-list"]/div[@class="info-item"][3]/text()').extract()  # 店铺详细地址

        # 进入公司主页，抽取公司介绍信息
        comp_url = self.domain + response.xpath('//div[@class="face-detail"]/h4/a/@href').extract()[0]
        yield Request(url=comp_url, meta={"i": item}, callback=self.parse_company)

    def parse_company(self, response):
        item = response.meta["i"]
        item["comprofile"] = response.xpath('//p[@class="com-jj"]/text()').extract()
        yield item

