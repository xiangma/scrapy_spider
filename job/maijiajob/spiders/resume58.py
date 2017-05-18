# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from maijiajob.items import Resume58Item
import re
from scrapy import log
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import os
from urllib import unquote
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

class Resume58Spider(scrapy.Spider):
    name = "resume58"
    download_delay = 3
    start_urls = (
        'http://www.58.com/zptaobao/changecity/',
    )

    def __init__(self, para=None, *args, **kwargs):
        super(Resume58Spider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
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
                fp = open(feed_uri+'.xz','rb')
                filename = feed_uri.split('/')[-1]
                content = fp.read()
                pyproducer('resume',bytes(filename),content)
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
            citys = self.para.split(',')
            for city in citys:
                city_code = city.split(':')[0]
                city_name = city.split(':')[1]
                if city_name.startswith('%'):
                    city_name = unquote(city_name)
                url = 'http://' + city_code + '.58.com/zptaobao/'
                yield Request(url=url, meta={"city": city_name,'city_code':city_code}, callback=self.parse_resume_tab)
        else:
            for selector in response.xpath('//dl[@id="clist"]/dd/a'):
                url = selector.xpath('@href').extract()[0]
                city = selector.xpath('text()').extract()[0]
                yield Request(url=url, meta={"city": city}, callback=self.parse_resume_tab)

    def parse_resume_tab(self, response):
        """
        进入城市主页，默认选中的tab是职位，需要切换到简历上
        """
        domain_matcher = r'(http://.*\.com).*'
        city = response.meta["city"]
        city_code = response.meta['city_code']

        domain = re.findall(domain_matcher, response.url)[0]
        resume_path = response.xpath('//a[@class="tabB"]/@href').extract()[0]
        url = domain + resume_path
        log.msg("resume tab:" + url)
        yield Request(url=url, meta={"city": city,'city_code':city_code}, callback=self.parse_category)

    def parse_category(self, response):
        """
         抓取八大类目职位信息： 淘宝客服， 淘宝美工， 店铺推广， 网店店长，文案编辑，活动策划，仓库管理，快递员
        """
        city = response.meta["city"]
        city_code = response.meta['city_code']

        taobaokefu = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaokefu")]/@href').extract()[0]
        yield Request(url=taobaokefu, meta={"category": "客服", "city": city}, callback=self.parse_resume_list)
        dianzhang = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaodianzhang")]/@href').extract()[0]
        yield Request(url=dianzhang, meta={"category": "运营管理", "city": city}, callback=self.parse_resume_list)
        meigong = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaomeigong")]/@href').extract()[0]
        yield Request(url=meigong, meta={"category": "美工设计", "city": city}, callback=self.parse_resume_list)
        tuiguang = response.xpath('//ul[@class="seljobCate"]/li/a[contains(@href, "taobaotuiguang")]/@href').extract()[0]
        yield Request(url=tuiguang, meta={"category": "营销推广", "city": city}, callback=self.parse_resume_list)

        wenanbianji = 'http://'+city_code+'.58.com/qzzptaobaobianji/?PGTID=14334901714760.06205486971884966&ClickID=1'
        yield Request(url=wenanbianji, meta={"category": u"文案编辑", "city": city}, callback=self.parse_resume_list)
        huodongcehua = 'http://'+city_code+'.58.com/qzzptaobaocehua/?PGTID=14334901674130.9351448225788772&ClickID=1'
        yield Request(url=huodongcehua, meta={"category": u"活动策划", "city": city}, callback=self.parse_resume_list)
        cangkuguanli = 'http://'+city_code+'.58.com/searchjob/?key=%E4%BB%93%E5%BA%93%E7%AE%A1%E7%90%86'
        yield Request(url=cangkuguanli, meta={"category": u"仓库管理", "city": city}, callback=self.parse_resume_list)
        kuaidiyuan = 'http://'+city_code+'.58.com/searchjob/?key=%E5%BF%AB%E9%80%92%E5%91%98'
        yield Request(url=kuaidiyuan, meta={"category": u"快递员", "city": city}, callback=self.parse_resume_list)

    def parse_resume_list(self, response):
        city = response.meta["city"]
        category = response.meta["category"]
        resume_list = response.xpath('//*[@id="infolist"]/dl')
        if len(resume_list) > 0:
            for selector in resume_list:
                date_matcher = r'[0-9]{2}-[0-9]{2}'
                refresh_date = selector.xpath('dd/text()')[-1].extract().strip()  # 更新时间
                date = re.findall(date_matcher, refresh_date.strip())
                if date or u'天前' in refresh_date:
                    log.msg('stop crawl, refresh date:' + date[0])
                    return
                else:
                    url = selector.xpath('dt/a/@href').extract()[0]
                    yield Request(url=url, meta={"category": category, "city": city}, callback=self.parse_resume_detail)

            # 需要分页
            next_page = response.xpath('//div[@class="pager"]/a[@class="next"]/@href').extract()  # 判断下一页链接是否存在
            refresh_date = resume_list[-1].xpath('dd/text()')[-1].extract().strip()  # 更新时间
            log.msg('refresh date:' + refresh_date)
            if next_page and refresh_date:
                domain_matcher = r'(http://.*\.com).*'
                date_matcher = r'[0-9]{2}-[0-9]{2}'
                date = re.findall(date_matcher, refresh_date.strip())
                if date or u'天前' in refresh_date:  # 如果以日期格式显示，说明简历已经不是最新的，不再继续抓取
                    log.msg("stop crawl, refresh date:" + date[0])
                    return
                else:
                    domain = re.findall(domain_matcher, response.url)[0]
                    url = domain + next_page[0]
                    log.msg("nextpage:" + next_page[0])
                    yield Request(url=url, meta={'city': city, 'category': category}, callback=self.parse_resume_list)

    def parse_resume_detail(self, response):
        # 检查是否被重定向到其它页面
        if "http://jianli.58.com/resume" not in response.url:
            log.msg("page wrong:" + response.url)
            return

        item = Resume58Item()
        item["url"] = response.url
        item["city"] = response.meta["city"]
        item["category"] = response.meta["category"]
        item["name"] = response.xpath('//span[@class="name"]/text()').extract()
        sex_age = response.xpath('//span[@class="sexAge"]/text()').extract()[0]   # （男，21岁）
        sex_age = sex_age.encode('utf8')
        item["gender"] = re.findall(r'（(.*)，.*）', sex_age)
        item["age"] = re.findall(r'（.*，(.*)）', sex_age)
        item["title"] = response.xpath('//span[@class="jobGoal"]/text()').extract()
        basic_info = response.xpath('(//ul[@class="expectDetail"])[1]')
        item["degree"] = basic_info.xpath('li[1]/text()').extract()
        item["work_years"] = basic_info.xpath('li[3]/text()').extract()
        item["live_city"] = basic_info.xpath('li[5]/text()').extract()
        item["hometown"] = basic_info.xpath('li[7]/text()').extract()
        exp_info = response.xpath('(//ul[@class="expectDetail"])[2]')
        item["exp_pos"] = exp_info.xpath('li[1]/text()').extract()
        item["exp_city"] = exp_info.xpath('li[3]/text()').extract()
        item["exp_salary"] = exp_info.xpath('li[5]/text()').extract()
        item["self_intro"] = response.xpath('//div[@class="intrCon"]/text()').extract()  # 自我介绍
        item["work_exp"] = response.xpath('//div[@class="addcont addexpe"]').extract()  # 大字段 工作经历
        item["edu_exp"] = response.xpath('//div[@class="addcont addeduc"]').extract()  # 大字段  教育经历
        item["lang_skill"] = response.xpath('//div[@class="addcont addlan"]').extract()  # 大字段 掌握语言
        item["cert"] = response.xpath('//div[@class="addcont addcert"]').extract()    # 大字段  获得证书
        item["ability"] = response.xpath('//div[@class="addcont addAbility"]').extract()  # 大字段  专业能力
        item["showme"] = response.xpath('//div[@class="addcont addshowme"]').extract()  # 大字段 个人作品
        item["refresh_time"] = response.xpath('//a[contains(@class,"refreshTime")]/text()').re(r'(\d{4}-\d{2}-\d{2})')
        yield item
