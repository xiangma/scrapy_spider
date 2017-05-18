# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from maijiajob.items import Resume58Item
import re
from scrapy import log
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy import log
from scrapy.core.downloader import Downloader, Slot
import os
from urllib import unquote
import random
import time
import json
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

class ResumegouItem(scrapy.Item):
    origin = scrapy.Field()
    url = scrapy.Field()
    city = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    name = scrapy.Field()
    gender = scrapy.Field()
    photo = scrapy.Field()
    birthday = scrapy.Field()
    hometown = scrapy.Field()
    age = scrapy.Field()
    live_city = scrapy.Field()
    degree = scrapy.Field()
    phone = scrapy.Field()
    self_intro = scrapy.Field()  # 自我评价
    work_years = scrapy.Field()  #工作时间
    exp_mode = scrapy.Field()
    exp_city = scrapy.Field()
    exp_pos = scrapy.Field()
    exp_industry = scrapy.Field()
    exp_salary = scrapy.Field()
    work_exp = scrapy.Field()  # 工作经历
    work_exp_ds = scrapy.Field()  
    edu_exp = scrapy.Field()
    lang_skill = scrapy.Field()  # 语言能力
    cert = scrapy.Field()  # 获取证书 certification
    ability = scrapy.Field()  # 专业技能
    showme = scrapy.Field()  # 作品 show me
    refresh_time = scrapy.Field()
    
    #html_filepath = scrapy.Field()
    #doc_filepath = scrapy.Field()
    #spark_point = scrapy.Field()  # 亮点


city_dict = { 
    'BeiJing'   :   '1',
    'ShangHai'  :   '2',
    'ShenZhen'  :   '3',
    'HangZhou'  :   '4',
    'GuangZhou' :   '5'
}


def fmt_text(str):
    if str.strip() == "":
        return ''

    str = str.strip().replace(',','%2C').replace('\n', '\\n')
    return str

class ResumegouSpider(scrapy.Spider):
    name = "resumegou"
    download_delay = 1
    download_concurrence = 1
    #max_company_list_page = 20
    #CRAWL_DAYS = 100

    start_urls = ["http://www.zhaopingou.com/"]
    #start_urls = ["http://www.zhaopingou.com/find_city?=%s"%(str(int(time.time()*1000)))]

    user_agent = random.choice(['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36',
                           "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
                           "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
                           "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
                           "Mozilla/5.0 (Windows; U; WinNT4.0; en-US; rv:1.2b) Gecko/20021001 Phoenix/0.2",
                           "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.23) Gecko/20090825 SeaMonkey/1.1.18",
                           "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/527  (KHTML, like Gecko, Safari/419.3) Arora/0.6 (Change: )",
                           "Mozilla/5.0 (Windows; U; ; en-NZ) AppleWebKit/527  (KHTML, like Gecko, Safari/419.3) Arora/0.8.0",
                           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Beamrise/17.2.0.9 Chrome/17.0.939.0 Safari/535.8",
                           "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/18.6.872.0 Safari/535.2 UNTRUSTED/1.0 3gpp-gba UNTRUSTED/1.0",
                           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
                           "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
                           "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
                           "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:10.0.1) Gecko/20100101 Firefox/10.0.1",
                           "Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20120403211507 Firefox/12.0",
                           "Mozilla/5.0 (Windows NT 6.0; rv:14.0) Gecko/20100101 Firefox/14.0.1",
                           "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:15.0) Gecko/20120427 Firefox/15.0a1",
                           "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0) Gecko/16.0 Firefox/16.0",
                           "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.1 (KHTML, like Gecko) Maxthon/3.0.8.2 Safari/533.1",
                           "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
                           "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
                           "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)",
                           "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/4.0)",
                           "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
                           "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/5.0)",
                           "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
                           "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.2; Trident/5.0)",
                           "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.2; WOW64; Trident/5.0)",
                           "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)",
                           "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/6.0)"])

    def __init__(self,para=None,para2=None,*args,**kwargs):
        super(ResumegouSpider, self).__init__(*args,**kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_started, signal=signals.engine_started)

        self.para = para
        self.citys = para.split(',')
        if para2:
            self.CRAWL_DAYS = int(para2)

    def engine_started(self):
        log.msg('[para] %s' %(self.para))
        self.crawler.engine.downloader.slots['zhaopingou.com'] = Slot(self.download_concurrence, self.download_delay, self.settings)

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

    def parse(self,response):
        yield self.next_city()

    def next_city(self):
        try:
            if len(self.citys)>0:
                city = self.citys.pop()
                cityId = city_dict[city]
                now_time = str(int(time.time()*1000))
                tt = str(time.time()).split('.')[0]
                url = 'http://www.zhaopingou.com/zhaopingou_interface/update_last_city?timestamp=%s'%(now_time)
                return scrapy.FormRequest(url,
                               headers = {'user_Agent':self.user_agent},
                               formdata = {'cityId':city_dict[city],
                                           'clientNo':'',
                                           'clientType':'2',
                                           'userToken':''},
                               meta = {'city':city},
                               callback = self.parse_city
                               )
            else:
                return None
        except Exception,e:
            log.err(e)

    def next_resume_list_page(self,page_num,city):
        now_time = str(int(time.time()*1000))
        url = 'http://www.zhaopingou.com/zhaopingou_interface/find_warehouse_by_position?timestamp=%s'%(now_time)
        return scrapy.FormRequest(url,
                       headers = {'user_Agent':self.user_agent},
                       formdata={'positionId':'0','pageSize':str(page_num),'pageNo':'25','sortType':'8','keyStr':'','degreesType':'0','startAge':'0','endAge':'0','gender':'-1','is_selected':'-1',\
                                 'address':'','isRead':'-1','isDay':'2','beginExperience':'-1','endExperience':'-1','articleId':'0','clientNo':'','clientType':'4','userToken':''},
                       meta = {'city':city,'page_num':int(page_num)+1,'referer':'http://www.zhaopingou.com/pn=%s'%(str(int(page_num)-1))},
                       cookies=[{'name':'zhaopingou_select_city','value':int(city_dict[city])},
                                {'name':'Hm_lpvt_3516a9fc0078d7f33534c71338ebe899','value':str(time.time()).split('.')[0]},
                                {'name':'Hm_lvt_3516a9fc0078d7f33534c71338ebe899','value':str(time.time()).split('.')[0]}
                                ],
                       callback = self.parse_resume_list
                       )

    def parse_city(self,response):
        log.msg(response.body)
        city = response.meta['city'] 
        try:
            now_time = str(int(time.time()*1000))
            url = 'http://www.zhaopingou.com/zhaopingou_interface/find_warehouse_by_position?timestamp=%s'%(now_time)
            return scrapy.FormRequest(url,
                           headers = {'user_Agent':self.user_agent},
                           formdata={'positionId':'0','pageSize':'0','pageNo':'25','sortType':'8','keyStr':'','degreesType':'0','startAge':'0','endAge':'0','gender':'-1','is_selected':'-1',\
                                     'address':'','isRead':'-1','isDay':'2','beginExperience':'-1','endExperience':'-1','articleId':'0','clientNo':'','clientType':'2','userToken':''},
                           meta = {'city':city,'page_num':'1','referer':'http://www.zhaopingou.com/'},
                           callback = self.parse_resume_list
                           )
        except Exception,e:
            log.err(e)

    def parse_resume_list(self,response):
        page_num = response.meta['page_num']
        city = response.meta['city']

        log.msg('[city:%s],page:%s'%(city,str(page_num)))
        log.msg(response.url)

        data = json.loads(response.body)
        try:
            for item in data['warehouseList']:
                resume = ResumegouItem()
                resume['origin'] = 'zhaopingou'

                resume['city'] = city
                resume['category'] = ''
                resume['title'] = ''
                resume['name'] = item['content']
                resume['gender'] = '男' if item['gender'] == 0 else '女'

                resume['phone'] = item['mobile']

                resume['work_years'] = item['experience']

                resumeHtmlId = item['resumeHtmlId']
                now_time = str(int(time.time()*1000))
                url = "http://www.zhaopingou.com/zhaopingou_interface/zpg_find_resume_html_details?timestamp=%s"%(now_time)
                yield scrapy.FormRequest(url,
                                         formdata={'resumeHtmlId':str(resumeHtmlId),'clientNo':'','clientType':'2','userToken':''},
                                         meta = {'resume':resume},
                                         callback = self.parse_resume
                                         )
            if len(data['warehouseList']) == 25:
                yield self.next_resume_list_page(page_num,city)
            else:
                yield self.next_city()
                
        except Exception,e:
            log.err(e)
            yield self.next_city()
        
    def parse_resume(self,response):
        resume = response.meta['resume']
        data = json.loads(response.body)['resumeHtml']
        try:
            log.msg('http://www.zhaopingou.com/resume/detail?source=1&resumeId=%s'%(data['id']))

            resume['url'] = 'http://www.zhaopingou.com/resume/detail?source=1&resumeId=%s'%(data['id'])

            resume['photo'] = data['resumeImg']
            resume['birthday'] = data['birthday']
            resume['hometown'] = data['residence']
            resume['age'] = data['age']
            resume['live_city'] = data['address']
            resume['degree'] = data['degreesName']

            if data.has_key('evaluate'):
                resume['self_intro'] = data['evaluate']

            resume['exp_mode'] = ''
            resume['exp_city'] = data['hopeAddress']
            log.msg(resume['exp_city'])
            resume['exp_pos'] = data['hopePosition']
            resume['exp_industry'] = data['hopeIndustry']
            resume['exp_salary'] = data['hopeSalary']
            tmplist = []
            tmp_dict = {}
            tmp_dict['company'] = data['last_company']
            tmp_dict['position'] = data['last_company_pname']
            tmp_dict['time_range'] = data['last_company_time']
            tmplist.append(tmp_dict)
            resume['work_exp'] = json.dumps(tmplist,ensure_ascii=False)
            resume['work_exp_ds'] = ''
            resume['edu_exp'] = ''
            resume['lang_skill'] = ''
            resume['cert'] = ''
            resume['ability'] = data['skills']
            resume['showme'] = ''
            resume['refresh_time'] = data['crate_time']

            #resume['html_filepath'] = 'http://www.zhaopingou.com'+data['html_filepath']
            #resume['doc_filepath'] = 'http://www.zhaopingou.com'+data['doc_filepath']
            yield resume
        except Exception,e:
            log.err(e)

