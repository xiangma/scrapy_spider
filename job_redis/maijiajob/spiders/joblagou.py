#-*- coding:utf-8 -*-
from scrapy.item import Item
from scrapy.item import Field
from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.core.downloader import Downloader, Slot
from maijiajob.items import LagouItem
from scrapy_redis.spiders import RedisCrawlSpider
import urllib
import os
import json
import scrapy
import random
import datetime

city_dict = { 
    'BeiJing'   :   'http://www.lagou.com/gongsi/2-0-25?sortField=1#filterBox',
    'ShangHai'  :   'http://www.lagou.com/gongsi/3-0-25?sortField=1#filterBox',
    'GuangZhou' :   'http://www.lagou.com/gongsi/213-0-25?sortField=1#filterBox',
    'ShenZhen'  :   'http://www.lagou.com/gongsi/215-0-25?sortField=1#filterBox',
    'HangZhou'  :   'http://www.lagou.com/gongsi/6-0-25?sortField=1#filterBox',
    'ChengDu'   :   'http://www.lagou.com/gongsi/252-0-25?sortField=1#filterBox',
    'NanJing'   :   'http://www.lagou.com/gongsi/79-0-25?sortField=1#filterBox',
    'TianJin'   :   'http://www.lagou.com/gongsi/4-0-25?sortField=1#filterBox',
    'WuHan'     :   'http://www.lagou.com/gongsi/184-0-25?sortField=1#filterBox',
    'NingBo'    :   'http://www.lagou.com/gongsi/98-0-25?sortField=1#filterBox',
    'ChongQing' :   'http://www.lagou.com/gongsi/5-0-25?sortField=1#filterBox',
    'SuZhou'    :   'http://www.lagou.com/gongsi/80-0-25?sortField=1#filterBox',
    'XiAn'      :   'http://www.lagou.com/gongsi/298-0-25?sortField=1#filterBox',
    'XaMen'     :   'http://www.lagou.com/gongsi/129-0-25?sortField=1#filterBox',
    'ChangSha'  :   'http://www.lagou.com/gongsi/198-0-25?sortField=1#filterBox',
    'Changzhou' :   'http://www.lagou.com/gongsi/87-0-0?sortField=1#filterBox'
}

def fmt_text(str):
    if str.strip() == "":
        return ''

    str = str.strip().replace(',','%2C').replace('\n', '\\n')
    return str

class JobLagouSpider(RedisCrawlSpider):
    name = 'joblagou'
    download_concurrence = 1
    max_company_list_page = 20
    CRAWL_DAYS = 100
    citys = ["BeiJing","ShangHai","GuangZhou","ShenZhen","HangZhou","ChengDu","NanJing","TianJin","WuHan","NingBo","ChongQing","SuZhou","XiAn","XaMen","ChangSha","Changzhou"]
    #start_urls = ["http://www.lagou.com/"]
    redis_key = 'lagou:start_urls'

    def parse(self,response):
        yield self.next_city()

    def next_city(self):  
        if len(self.citys)>0:
            city = self.citys.pop()
            url = city_dict[city]
            url = url.replace('?sortField=1#filterBox','.json')+'?first=false&pn=1&sortField=1&havemark=0'          
            print('-------1111-------next_city---[city:%s],company list url:%s,page:1------------'%(city,url))
            return Request(url = url,
                           cookies= {'JSESSIONID':'57286E4782F551C26D97863CD37F5E3F', 'user_trace_token':'20170510085916-e428d1c4-351b-11e7-8385-525400f775ce', 'LGUID':'20170510085916-e428d534-351b-11e7-8385-525400f775ce', '_putrc':'8C060BCF074174C3', 'login':'true', 'unick':'%E9%A9%AC%E7%BF%94', 'showExpriedIndex':'1', 'showExpriedCompanyHome':'1', 'showExpriedMyPublish':'1', 'hasDeliver':'95', 'TG-TRACK-CODE':'index_message', 'index_location_city':'%E5%85%A8%E5%9B%BD', 'Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6':'1494377951', 'Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6':'1494384114', '_gat':'1', 'LGSID':'20170510104159-3d6b6e57-352a-11e7-83f7-525400f775ce', 'PRE_SITE':'https%3A%2F%2Fwww.lagou.com%2Fgongsi%2F198-0-25%3Ffirst%3Dfalse%26pn%3D1%26sortField%3D1%26havemark%3D0', 'PRE_LAND':'https%3A%2F%2Fwww.lagou.com%2Fgongsi%2F198-0-25%3Ffirst%3Dfalse%26pn%3D1%26sortField%3D1%26havemark%3D0', 'LGRID':'20170510104159-3d6b6fad-352a-11e7-83f7-525400f775ce'},
                           # headers = {'user_Agent':self.user_agent},
                           callback = self.parse_company_list,
                           meta = {'city':city,'page_num':1},
                           )
        else:
            return None

    def next_company_list_page(self,page_num,city):
        print('------222---77777------next_company_list_page------------')
        url = city_dict[city]
        page_num += 1
        url = url.replace('?sortField=1#filterBox','.json')+'?first=false&pn='+str(page_num)+'&sortField=1&havemark=0'
        return Request(url = url,
                       callback = self.parse_company_list,
                       meta = {'city':city,'page_num':page_num},
                       )

    def next_position_page(self,companyId,position_page,job):
        print('----------88888888------next_position_page------------------------')
        position_page += 1
        url = "http://www.lagou.com/gongsi/searchPosition.json?companyId="+str(companyId)+"&positionFirstType=%E5%85%A8%E9%83%A8&pageNo="+str(position_page)+"&pageSize=10"
        return Request(url = url,
                      callback = self.parse_position_id,
                      meta = {'position_page':position_page,'job':job,'companyId':companyId},
                      dont_filter=True
                      )


    def parse_company_list(self,response):
        print('----------222----------parse_company_list--------------------------')
        city = response.meta['city']
        page_num = response.meta['page_num']
        data = json.loads(response.body)
        # print('----------------------Start grab company list page:%s,total company:%s,city:%s-----------'%(str(page_num),str(len(data['result'])),city))
        print('--------------[url:%s]-----------------'%(response.url))

        try:
            for company in data['result']:
                job = LagouItem()
                companyId = company['companyId']
                job['city'] = company['city']
                job['industry'] = company['industryField']
                job['logo'] = company['companyLogo']
                job['company'] = company['companyFullName']
                url = 'http://www.lagou.com/gongsi/%s.html' %(str(companyId))  #获得当页公司的URL
                print('-----!!!get company url-------[city:%s],company url:%s,page:%s---------------' %(city,url,str(page_num)))
                yield Request(url = url,
                              callback = self.parse_company,
                              meta = {'job':job,'companyId':companyId,'company_page':1},
                              dont_filter=True
                              )
            if int(data['pageSize']) == 16 and page_num < self.max_company_list_page:
                yield self.next_company_list_page(page_num,city)
                #print(str(data['pageSize']))
            else:
                yield self.next_city()
        except Exception, e:
            print('--------!!!!!!!err!!!!--------')
            print(e.message)
            yield self.next_city()
                    
    def parse_company(self,response):
        
        companyId = response.meta['companyId']
        job = response.meta['job']

        try:
            print('-----22----3333------parse_company----start-----')
            job['nature'] = response.xpath('//*[@id="basic_container"]/div[@class="item_content"]/ul/li[2]/span/text()').extract()[0]
            # job['comprop'] = response.xpath('//*[@class="item_content"]/ul/li[1]/span/text()').extract()[0]
            job['comprofile'] = response.xpath('//*[@id="company_intro"]/div[@class="item_content"]/div[@class="company_intro_text"]/span[@class="company_content"]/p/text()').extract()[0]
            job['shopurl'] = response.xpath('//h1/a/@href').extract()[0]
            job['scale'] = response.xpath('//*[@id="basic_container"]/div[@class="item_content"]/ul/li[3]/span/text()').extract()[0]
            if response.xpath('//*[@class="mlist_li_desc"]/text()'):
                job['shopaddr'] = fmt_text(response.xpath('//*[@class="mlist_li_desc"]/text()').extract()[0])
            url = "http://www.lagou.com/gongsi/searchPosition.json?companyId="+str(companyId)+"&positionFirstType=%E5%85%A8%E9%83%A8&pageNo=1&pageSize=10"  #获得该公司所有职位URL列表
            print('-----22----3333------nonononononononoonononononono-----')
            yield Request(url = url,
                          callback = self.parse_position_id,
                          meta = {'job':job,'position_page':1,'companyId':companyId},
                          dont_filter=True
                          )
        except Exception,e:
            print(e.message)

    def parse_position_id(self,response):
        print('-------44444-------parse_position_id---------------------')
        companyId = response.meta['companyId']
        position_page = response.meta['position_page']
        job = response.meta['job']

        data = json.loads(response.body)['content']['data']['page']
        results = data['result']

        print('Start grab position list page:%s,total position:%s,company:%s'%(position_page,len(results),companyId))
        print('[url:%s]'%(response.url))

        for result in results:
            job['publish'] = result['createTime']
            positionId = result['positionId']
            if ":" in result['createTime']:
                url = 'http://www.lagou.com/jobs/%s.html' %(str(positionId))
                yield Request(url = url,
                              callback = self.parse_position,
                              meta = {'job':job},
                              dont_filter=True
                              )
            else:               
                publish = datetime.datetime.strptime(result['createTime'],'%Y-%m-%d')
                crawlDays = datetime.timedelta(days=self.CRAWL_DAYS)
                start_crawl_day = datetime.datetime.now() - crawlDays
                if (publish < start_crawl_day):
                    print('continue')
                    continue
                    #print(start_crawl_day)
                    #flag = 1
                    #break
                else:
                    url = 'http://www.lagou.com/jobs/%s.html' %(str(positionId))
                    yield Request(url = url,
                                  callback = self.parse_position,
                                  meta = {'job':job},
                                  dont_filter=True
                                  )

        #print('______________%d'%(flag))
        #if flag == 1:
        #    return

        if len(results) == 10:
            yield self.next_position_page(companyId,position_page,job)

    def parse_position(self,response):
        print('-------55555-----parse_position-----------------')
        job = response.meta['job']

        job['url'] = response.url
        job['depart'] = response.xpath('//*[@class="job-name"]/div[@class="company"]/text()').extract()[0].strip()
        job['lure'] = response.xpath('//*[@class="job-advantage"]/p/text()').extract()[0].strip()
        job['name'] = fmt_text(response.xpath('//*[@class="job-name"]/span[@class="name"]/text()').extract()[0]).strip()
        job_request = response.xpath('//*[@class="job_request"]/p[1]/span')
        job['salary'] = job_request[0].xpath('text()').extract()[0].strip()
        job['experience'] = job_request[2].xpath('text()').extract()[0].replace('/','').strip()
        job['education'] = job_request[3].xpath('text()').extract()[0].replace('/','').strip()
        job['prop'] = job_request[4].xpath('text()').extract()[0].strip()
        job['desc'] = response.xpath('//*[@class="job_bt"]/div').extract()[0].replace('<div>','').replace('</div>','').replace('<p>','').replace('</p>','').replace('<br>','').strip()
        job['origin'] = 'lagou'
        print("---------------------item---------------------")

        yield job