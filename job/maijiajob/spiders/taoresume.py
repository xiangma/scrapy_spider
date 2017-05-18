# -*- coding: utf-8 -*-
import scrapy
import urllib2
import json
from scrapy import log
from scrapy import Request
from datetime import datetime
from datetime import timedelta
from maijiajob.items import ResumetaoItem
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals 
import os

craw_config = {
    'day2' : False,
    'day7' : True
    }
class TaoresumeSpider(scrapy.Spider):
    name = "taoresume"
    download_delay = 3
    #cookie_server = '127.0.0.1:8888'
    cookie_server = '115.231.94.169:8888'
    start_urls = (
        'http://zhaopin.taobao.com',
    )
    domain = 'http://zhaopin.taobao.com'
    url_tmp = 'http://zhaopin.taobao.com/resume/resumeSearch.htm?' \
              'workMode=2&jobType=%s&workExperience=0&salary=0&p=1#J_Search'
    # 需要搜索的jobType
    jobTyes = [6, 8, 10, 21, 22, 5, 31, 1, 63]
    typeMap = {6: "售后客服", 8: "售前客服", 10: "电话客服", 21: "网页设计", 22: "图片设计", 5: "市场推广", 31: "活动策划", 1: "店长", 63: "运营主管"}
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() + timedelta(hours=8) - timedelta(hours=24)).strftime("%Y-%m-%d")
    sevendaysago = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=7)).strftime("%Y-%m-%d")

    para = ''

    def __init__(self, para=None, *args, **kwargs):
        super(TaoresumeSpider, self).__init__(*args, **kwargs)
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
        url = 'http://zhaopin.taobao.com/resume/resumeSearch.htm'
        res = urllib2.urlopen("http://%s/taojob.json" % self.cookie_server, timeout=30)
        if res.getcode() == 200:
            cookies = json.loads(res.readline())
            log.msg("get login cookie success")
        else:
            log.msg("get login cookie failed")
            return

        yield Request(url=url, cookies=cookies, callback=self.parse_search_page)

    def parse_search_page(self, response):
        if 'user/signin.htm' in response.url:
            log.msg('login failed')
            return

        for jobType in self.jobTyes:
            url = self.url_tmp % jobType
            yield Request(url=url, meta={"jobType": jobType}, callback=self.parse_list)

    def parse_list(self, response):
        """只抓取今天和昨天更新的简历，如果当前页的最后一条简历的更新时间是今天，则需抓取下一页"""
        jobType = response.meta["jobType"]
        selectors = response.xpath('//table[@class="job-table"]/tr[position()>1]')
        refresh_time = ''
        for sel in selectors:
            url = self.domain + sel.xpath('td[1]/div/a/@href').extract()[0]
            refresh_time = sel.xpath('td[8]/text()').extract()[0].strip()
            #if refresh_time == self.today or refresh_time == self.yesterday:
            if (craw_config['day2'] and refresh_time>= self.yesterday)or(craw_config['day7'] and refresh_time>= self.sevendaysago):
                yield Request(url=url, meta={"jobType": jobType, "refresh_time": refresh_time}, callback=self.parse_resume)
            else:
                log.msg("stop crawl, refresh time is too late:" + refresh_time)
                return

        next_page_path = response.xpath('//div[@id="pager"]/ul/li[last()]/a/@href').extract()[0]
        log.msg('refresh_time:' + refresh_time)
        if "javascript" not in next_page_path and (refresh_time == self.today or refresh_time == self.yesterday):
            next_page_url = 'http://zhaopin.taobao.com/resume/resumeSearch.htm' + next_page_path
            log.msg('reqeust next page:' + next_page_path + " refresh time:" + refresh_time)
            yield Request(url=next_page_url, meta={"jobType": jobType}, callback=self.parse_list)
        else:
            log.msg('stop crawl next page')


    def parse_resume(self, response):
        jobType = response.meta["jobType"]
        item = ResumetaoItem()
        item["url"] = response.url
        item["refresh_time"] = response.meta["refresh_time"]
        item["category"] = self.typeMap.get(jobType)
        # 基本信息部分
        item["photo"] = response.xpath('//img[@class="guide-info-avatarL"]/@src').extract()
        item["name"] = response.xpath('//div[contains(@class, "real-name")]/text()').extract()
        item["gender"] = response.xpath('(//div[@class="form-segment-body"])[1]/div[2]/div/text()').extract()
        item["live_city"] = response.xpath('(//div[@class="form-segment-body"])[1]/div[3]/div/text()').extract()
        item["birthday"] = response.xpath('(//div[@class="form-segment-body"])[1]/div[4]/div/text()').extract()
        item["phone"] = response.xpath('(//div[@class="form-segment-body"])[1]/div[5]/div/text()').extract()
        item["work_years"] = response.xpath('(//div[@class="form-segment-body"])[1]/div[6]/div/text()').extract()

        # 求职意向
        item["exp_mode"] = response.xpath('(//div[@class="form-segment-body"])[2]/div[1]/div/text()').extract()
        item["exp_city"] = response.xpath('(//div[@class="form-segment-body"])[2]/div[2]/div/text()').extract()
        # 工作岗位, 如果有多个用逗号连接
        item["exp_pos"] = response.xpath('(//div[@class="form-segment-body"])[2]/div[3]/div/div/text()').extract()  #
        item["exp_pos"] = map(lambda x: x.strip(), item["exp_pos"])
        item["exp_pos"] = ",".join(item["exp_pos"])
        # 期望行业, 如果有多个用逗号连接
        item["exp_industry"] = response.xpath('(//div[@class="form-segment-body"])[2]/div[4]/div/div/text()').extract()
        item["exp_industry"] = map(lambda x: x.strip(), item["exp_industry"])
        item["exp_industry"] = ",".join(item["exp_industry"])
        item["exp_salary"] = response.xpath('(//div[@class="form-segment-body"])[2]/div[5]/div/text()').extract()
        # ****************************************************
        # 以下部分不能单纯通过div的次序定位，需要使用title定位
        # 先把整块内容抽出来，在pipeline中做后续处理
        #***************************************************
        segments = response.xpath('//div[@class="form-segment-box"]')
        for segment in segments:
            title = segment.xpath('div[@class="form-segment-title"]/h4/text()').extract()[0].strip()
            if title == u"电商从业经验":
                item["work_exp_ds"] = segment.xpath('div[@class="form-segment-body"]').extract()
            if title == u"非电商从业经历":
                item["work_exp"] = segment.xpath('div[@class="form-segment-body"]').extract()
            if title == u"教育经历":
                item["edu_exp"] = segment.xpath('div[@class="form-segment-body"]').extract()
        yield item


