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
from scrapy.core.downloader import  Slot
from kafka import KafkaConsumer, KafkaProducer

category_url = {
    '设计主管':'zhaopin/s/_%E8%AE%BE%E8%AE%A1%E4%B8%BB%E7%AE%A1/',
    '平面设计':'zhaopin/s/_%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1/',
    'UI设计':'zhaopin/s/_ui%E8%AE%BE%E8%AE%A1/',
    '美工助理':'zhaopin/s/_%E7%BE%8E%E5%B7%A5%E5%8A%A9%E7%90%86/',
    '设计总监':'zhaopin/s/_%E8%AE%BE%E8%AE%A1%E6%80%BB%E7%9B%91/',
    '版师':'zhaopin/s/_%E7%89%88%E5%B8%88/',
    '摄影师':'zhaopin/s/_%E6%91%84%E5%BD%B1%E5%B8%88/',
    '模特':'zhaopin/s/_%E6%A8%A1%E7%89%B9/',
    '化妆师':'zphuazhuangshi/',
    '彩妆师':'zphuazhuangshi/_%E5%BD%A9%E5%A6%86%E5%B8%88/zhaopin/',
    '运营总监':'zhaopin/s/_%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91/',
    '品牌经理':'zhaopin/s/_%E5%93%81%E7%89%8C%E7%BB%8F%E7%90%86/',
    '采购经理':'zhaopin/s/_%E9%87%87%E8%B4%AD%E7%BB%8F%E7%90%86/',
    '淘宝运营':'zpdianpuyunying/_%E6%B7%98%E5%AE%9D%E8%BF%90%E8%90%A5/zhaopin/',
    '天猫运营':'zpdianpuyunying/_%E5%A4%A9%E7%8C%AB%E8%BF%90%E8%90%A5/zhaopin/',
    '京东运营':'zhaopin/s/_%E4%BA%AC%E4%B8%9C%E8%BF%90%E8%90%A5/?from=zhaopin_indexpage',
    '微信运营':'zhaopin/s/_%E5%BE%AE%E4%BF%A1%E8%BF%90%E8%90%A5/',
    '店长助理':'zhaopin/s/_%E5%BA%97%E9%95%BF%E5%8A%A9%E7%90%86/',
    '网站编辑':'zpwangzhanbianji/',
    '活动策划':'zhaopin/s/_%E6%B4%BB%E5%8A%A8%E7%AD%96%E5%88%92/',
    '采购':'zpcaigou/',
    '新媒体运营':'zhaopin/s/_%E6%96%B0%E5%AA%92%E4%BD%93%E8%BF%90%E8%90%A5/?from=zhaopin_indexpage',
    '销售总监':'zhaopin/s/_%E9%94%80%E5%94%AE%E6%80%BB%E7%9B%91/',
    '市场总监':'zhaopin/s/_%E5%B8%82%E5%9C%BA%E6%80%BB%E7%9B%91/',
    '网店推广':'zhaopin/s/_%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF/',
    '网络推广':'zhaopin/s/_%E7%BD%91%E7%BB%9C%E6%8E%A8%E5%B9%BF/',
    'SEO':'zhaopin/s/_SEO/',
    'SEM':'zhaopin/s/_SEM/',
    '销售经理':'zpxiaoshoujingli/',
    '客户经理':'zpkehuzongjian/',
    '电话销售':'zpdianhuaxiaoshou/',
    '市场推广':'zhaopin/s/_%E5%B8%82%E5%9C%BA%E6%8E%A8%E5%B9%BF/',
    '市场营销':'zhaopin/s/_%E5%B8%82%E5%9C%BA%E8%90%A5%E9%94%80/',
    '市场策划':'zhaopin/s/_%E5%B8%82%E5%9C%BA%E7%AD%96%E5%88%92/',
    '行政经理':'zhaopin/s/_%E8%A1%8C%E6%94%BF%E7%BB%8F%E7%90%86/',
    '前台':'zpqiantai/?original=%E5%89%8D%E5%8F%B0&websearchkw=%E5%89%8D%E5%8F%B0%2F%E6%80%BB%E6%9C%BA%2F%E6%8E%A5%E5%BE%85',
    '行政助理':'zpxingzhengzhuli/',
    '经理助理/文秘':'zpwenmiwenyuan/',
    '人事行政':'zhaopin/s/_%E4%BA%BA%E4%BA%8B%E8%A1%8C%E6%94%BF/?from=zhaopin_indexpage',
    '人事专员':'zprenshizhuli/?original=%E4%BA%BA%E4%BA%8B%E4%B8%93%E5%91%98&websearchkw=%E4%BA%BA%E4%BA%8B%E4%B8%93%E5%91%98%2F%E5%8A%A9%E7%90%86',
    '人事经理':'zhaopin/s/_%E4%BA%BA%E4%BA%8B%E7%BB%8F%E7%90%86/?from=zhaopin_indexpage',
    '人事主管':'zhaopin/s/_%E4%BA%BA%E4%BA%8B%E4%B8%BB%E7%AE%A1/',
    '出纳':'zpchuna/?websearchkw=%E5%87%BA%E7%BA%B3',
    '会计':'zhaopin/s/_%E4%BC%9A%E8%AE%A1/',
    '审计':'zhaopin/s/_%E5%AE%A1%E8%AE%A1/',
    '法务':'zhaopin/s/_%E6%B3%95%E5%8A%A1/',
    '产品经理':'zpchanpinjingli/',
    '技术总监':'zpjishuzongjian/',
    'web前端':'zhaopin/s/_web%E5%89%8D%E7%AB%AF/',
    '前端开发':'zhaopin/s/_%E5%89%8D%E7%AB%AF%E5%BC%80%E5%8F%91/',
    'ios':'zhaopin/s/_ios/',
    'Android':'zhaopin/s/_Android/',
    'java':'zhaopin/s/_java/',
    'PHP':'zhaopin/s/_PHP/',
    'DBA':'zpshujukuguanli/',
    '软件测试':'zpceshigongchengshi/',
    '网络工程师':'zhaopin/s/_%E7%BD%91%E7%BB%9C%E5%B7%A5%E7%A8%8B%E5%B8%88/',
    '运维':'zhaopin/s/_%E8%BF%90%E7%BB%B4/',

    '配货打包':'zhaopin/s/_%E9%85%8D%E8%B4%A7%E6%89%93%E5%8C%85/',
    '打包发货':'zhaopin/s/_%E6%89%93%E5%8C%85%E5%8F%91%E8%B4%A7/',

    '打包发货':'zptaobao/_%E6%89%93%E5%8C%85%E5%8F%91%E8%B4%A7/zhaopin/',
    '配货'    :'zptaobao/_%E9%85%8D%E8%B4%A7/zhaopin/',
    '打单'    :'zptaobao/_%E6%89%93%E5%8D%95/zhaopin/',
    '推广专员':'zptaobao/_%E6%8E%A8%E5%B9%BF%E4%B8%93%E5%91%98/zhaopin/',
    '直通车'  :'zptaobao/_%E7%9B%B4%E9%80%9A%E8%BD%A6/zhaopin/',
    '天猫推广':'zptaobao/_%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF/zhaopin/',
    '淘宝推广':'zptaobao/_%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF/zhaopin/',
    '运营主管':'zptaobao/_%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1/zhaopin/',
    '运营助理':'zptaobao/_%E8%BF%90%E8%90%A5%E5%8A%A9%E7%90%86/zhaopin/',
    '文案策划':'zpwenan/',
    '运营专员':'zptaobao/_%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98/zhaopin/',
    '天猫店长':'zptaobao/_%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF/zhaopin/',
    '淘宝店长':'zptaobao/_%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF/zhaopin/',
    '美工助理':'zptaobao/_%E7%BE%8E%E5%B7%A5%E5%8A%A9%E7%90%86/zhaopin/',
    '网页设计':'zpwangyesheji/',
    '售后客服':'zptaobao/_%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D/zhaopin/',
    '平面设计':'zppingmiansheji/',
    '天猫美工':'zptaobao/_%E5%A4%A9%E7%8C%AB%E7%BE%8E%E5%B7%A5/zhaopin/',
    '客服经理':'zpkefu/_%E5%AE%A2%E6%9C%8D%E7%BB%8F%E7%90%86/zhaopin/',
    '客服主管':'zpkefu/_%E5%AE%A2%E6%9C%8D%E4%B8%BB%E7%AE%A1/zhaopin/',                                                                                     
    '客服专员':'zpkefu/_%E5%AE%A2%E6%9C%8D%E4%B8%93%E5%91%98/zhaopin/',
    '天猫客服':'zptaobao/_%E5%A4%A9%E7%8C%AB%E5%AE%A2%E6%9C%8D/zhaopin/',
    '售后客服':'zptaobao/_%E5%94%AE%E5%90%8E%E5%AE%A2%E6%9C%8D/zhaopin/',
    '售前客服':'zptaobao/_%E5%94%AE%E5%89%8D%E5%AE%A2%E6%9C%8D/zhaopin/',

    '淘宝客服' : 'zptaobaokefu/',      #淘宝客服
    '淘宝美工' : 'zptaobaomeigong/',   #淘宝美工
    #营销推广
    '淘宝推广' : 'zptaobao/_%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF/zhaopin/', #淘宝推广

    #店铺管理
    '店铺管理' : 'zpdianpuguanli/', #店铺管理
    '店铺运营' : 'zpdianpuyunying/', #店铺运营
    '仓库管理' : 'zpcangkuguanli/',
    '快递员'   : 'zpkuaidi/',
    '摄影师'   : 'zptaobao/_%E6%91%84%E5%BD%B1%E5%B8%88/zhaopin/',
    '模特'     : 'zpmote/',

}

jianzhi_category_url = {
    #兼职
    '模特'     : 'jzjianzhimote/',
    '摄影师'   : 'jzsheyingshi/',
    '设计制作' : 'jzshejizhizuo/',
    '客服'     : 'qitajianzhi/_%E5%AE%A2%E6%9C%8D/jianzhi/',
    '快递员'   : 'qitajianzhi/_%E5%BF%AB%E9%80%92%E5%91%98/jianzhi/',
}

job_type_dict = { 
    1 : u'兼职',
    2 : u'实习',
    3 : u'全职',
            
}

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

class JobganjiSpider(Spider):
    name = "jobganji"
    download_delay = 3
    concurrent_requests = 3
    cookies_enabled = False
    start_urls = (
         'http://www.ganji.com/index.htm',
    )
    para = ''
    def __init__(self,para = None,*args,**kwargs):
        if para:
            #self.para = para.decode(sys.getdefaultencoding())
            self.para = urllib.unquote(para)
            log.msg('self para '+ self.para,_level='DEBUG')
        super(JobganjiSpider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)

    def engine_opened(self):
        log.msg(str(type(self.para)) + ' para:' + self.para)
        self.crawler.engine.downloader.slots['sou.zhaopin.com'] = Slot(5, 5, self.settings)

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
        """可以通过参数给需要爬取得城市，城市间以逗号分隔，不给参数就默认获取所有城市"""
        sel = Selector(response)
        allcities = sel.xpath('/html/body/div[1]/div[3]//a')
        reload(sys)
        sys.setdefaultencoding(response.headers['Content-Type'][response.headers['Content-Type'].find('charset=')+len('charset='):])
        
        if self.para:
            para_cities = self.para.split(u',')
            #这个地方没有指定编码，但能正确解码，奇怪。
            #print '-'*30,self.para,para_cities,type(para_cities)
            for para_city in para_cities:
                #print '-'*30,'para_city',para_city,type(para_city)#,para_city.encode('utf-8')
                cityname = para_city.strip()#.decode('utf-8')
                #print '-'*30,'cityname',type(cityname),cityname
                citynode = sel.xpath('/html/body/div[1]/div[3]//a[contains(text(),"%s")]'%cityname)
                log.msg(cityname+'-'*30+'citynode'+str(citynode),_level='DEBUG')
                #print cityname+'-'*30+'citynode'+str(citynode)

                cityurl = pick_xpath(citynode,'@href')
                log.msg('-'*30+cityurl,_level='DEBUG')
                yield Request(cityurl,meta={'cityurl':cityurl,'cityname':cityname},callback=self.parse_cities)
        else:
            for city in allcities:
                cityurl = pick_xpath(city,'@href')
                cityname = pick_xpath(city,'text()')
                log.msg('in loop city url '+cityurl + ' city name '+cityname,_level='DEBUG')

                yield Request(cityurl,meta={'cityurl':cityurl,'cityname':cityname},callback=self.parse_cities)

    def parse_cities(self,response):
            """对每个城市的那几类职位进行分别爬取"""
            sel = Selector(response)
            cityurl = response.meta['cityurl']
            cityname = response.meta['cityname']
            
            for key in category_url.iterkeys():
                category = key
                cate_url = category_url.get(key)
                #if key == '快递员':
                log.msg('cityurl+cateurl'+'-'*30+cityurl+cate_url,_level='DEBUG')
                yield Request(cityurl+cate_url,meta={'category':category,'cityurl':cityurl,'job_type':3,'cityname':cityname},callback=self.parse_job_list)
            
            for key in jianzhi_category_url.iterkeys():
                category = key
                cate_url = jianzhi_category_url.get(key)
                log.msg('cityurl+cateurl'+'-'*30+cityurl+cate_url,_level='DEBUG')
                yield Request(cityurl+cate_url,meta={'category':category,'cityurl':cityurl,'job_type':1,'cityname':cityname},callback=self.parse_job_list)


    def parse_job_list(self,response):
        """每一类职务的查询结果"""
        sel = Selector(response)
        cityurl = response.meta['cityurl']
        cityname = response.meta['cityname']
        category = response.meta['category']

        page_num = response.meta['page_num'] if 'page_num' in response.meta else 1

        job_list1=sel.xpath('//dl[@class="list-sty1-bg clearfix"]')
        job_list = sel.xpath('//dl[@class="list_noimg job-list"]|//dl[@class="list-noimg job-list clearfix"]')
        
        dd = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        
        log.msg('now :------------'+dd.strftime('%Y-%m-%d %H:%M:%S'))
        log.msg('INFO***cityname:'+cityname+'   category:'+category+'  page_num:'+str(page_num))
        #job_url = 'http://hz.ganji.com/jzshejizhizuo/1827724223x.htm' #测试用
       #yield Request(job_url,meta={'category':category,'cityurl':cityurl,'publish':'2015-10-20 09:00:00','cityname':cityname ,'job_type':response.meta['job_type']},callback=self.parse_job)
        #return
        #模板2列表
        for job in job_list1:
            refresh_date=pick_xpath(job,'.//*[@class="time mt-24"]/text()')
            refresh=True
            if refresh_date.count(u'月'):
                 refresh=False
            if refresh:
                job_url=pick_xpath(job,'.//*[@class="fl j-title"]/a/@href').strip('/')
                yield Request(cityurl+job_url,meta={'category':category,'cityurl':cityurl,'publish':refresh_date, 'cityname':cityname ,'job_type':response.meta['job_type']},callback=self.parse_job)
            else:
                return

        #模板1的列表
        for job in job_list:
            """赶集默认按时间排序，所以遇到第一个不满足的即可退出"""
            refresh_date = job.xpath('dd/text()')[-1].extract().strip()   # 更新时间
            try:
                refresh_date2 = datetime.datetime.utcfromtimestamp(int(pick_xpath(job,'./@pt')))
            except Exception,e:
                log.err('get job date failed! :' + str(e))
                log.msg(response.url+'\n'+pick_xpath(job,'./@pt'))
                refresh_date2 = None

            #log.msg('refresh_date :-----------'+ refresh_date)#.strftime('%Y-%m-%d %H:%M:%S'))

            refresh_date = re.findall(r'(\d{2}-\d{2})',refresh_date)
            #log.msg('yesterday_date:'+ str(yesterday_date) + '-'*30 + 'refresh_date:'+ str(refresh_date),_level='DEBUG')
            #cc = dd - refresh_date
            #if  cc.total_seconds() <= 86400: # 24小时内
            if not refresh_date: #要么是 今天 要么是 月-日
                job_url =  pick_xpath(job,'.//a[1]/@href')
                #log.msg('Request job url : '+job_url,_level='DEBUG')

                yield Request(job_url,meta={'category':category,'cityurl':cityurl,'publish':refresh_date2.strftime('%Y-%m-%d %H:%M:%S') if refresh_date2 else refresh_date, 'cityname':cityname ,'job_type':response.meta['job_type']},callback=self.parse_job)
            else:
                log.msg('spider stop'+ '-'*30 + 'refresh_date:'+ str(refresh_date),_level='DEBUG')

                return
            # 需要分页
        next_page = pick_xpath(sel,'//*[@class="next"]/@href')  # 判断下一页链接是否存在
        if next_page and page_num < 100:
            page_num += 1
            yield Request(cityurl+next_page,meta={'category':category,'cityurl':cityurl,'cityname':cityname,'job_type':response.meta['job_type'],'page_num':page_num},callback=self.parse_job_list)


    def parse_job(self,response):
        """获取每一份职位"""
        sel = Selector(response)
        item = JobItem()
        cityurl = response.meta['cityurl']
        job_type = response.meta['job_type']
        item["city"] = response.meta["cityname"]
        item["category"] = response.meta["category"]   #工作类别
        item["url"] = response.url
        item['prop'] = job_type_dict.get(response.meta["job_type"],u'全职')

        baseinfo_node = sel.xpath('//ul[@class="clearfix pos-relat"]')
        if not baseinfo_node:
            baseinfo_node = sel.xpath('//ul[@class="clearfix"]')
        if  baseinfo_node:
            pass
            #baseinfo_node = baseinfo_node[0]
        else:
            #没找到对应节点
            return
        #log.msg('baesinfo_node: '+str(baseinfo_node),_level='DEBUG')
        
        item['publish'] = response.meta['publish']

        tmp = u"职位名称"
        item['name'] = pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/a/text()'%tmp) # 职位名称

        # from scrapy.shell import inspect_response
        # inspect_response(response)

        tmp = u"最低学历"
        item['education'] = pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/text()'%tmp) #学历要求

        tmp = u"工作经验"
        item['experience'] = pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/text()'%tmp) #工作经验
        tmp = u"工作地点"
        item['address'] = pick_xpaths(baseinfo_node,'./li[contains(text(),"%s")]/em//text()'%tmp,"") #工作地点

        tmp = u"龄："
        # item[''] = pick_xpaths(baseinfo_node,'./li[contains(text(),"%s")]/em/text()'%tmp,"") #年龄
        tmp = u"薪"
        salary = pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/text()'%tmp).strip('（') #薪金

        #薪金部分页面情况复杂，不规范，不统一，暂针对这几种情况处理
        if salary:
            tmp = re.findall('\d+-\d+',salary)
            if tmp:
                item['salary'] = tmp[0]+ '元'
            else:
                #面议
                tmp = u"薪"
                item['salary'] = salary +  pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/span/text()'%tmp)
        else:
            salary =  pick_xpath(baseinfo_node,'./li[contains(text()[2],"%s")]/em/text()'%tmp).strip('（') 
            tmp = re.findall('\d+-\d+',salary)
            if tmp:
                item['salary'] = tmp[0]+ '元'
            else:
                #面议
                tmp = u"薪"
                item['salary'] = salary +  pick_xpath(baseinfo_node,'./li[contains(text()[2],"%s")]/em/span/text()'%tmp)

        item['desc'] = pick_xpaths(sel,'//*[@class="deta-Corp"]//text()','\n')       #职位描述
        if 1 == job_type:
            tmp = u'待遇'
            tmp_str = pick_xpath(baseinfo_node,'./li[contains(text(),"%s")]/em/text()'%tmp)
            item['salary'] = u'面议'
            item['desc'] = item['desc'] + '\n' + tmp_str

        item['comprofile'] = pick_xpaths(sel,'//*[@id="description-compy"]/text()|//*[@class="fc4b f14"]//text()','\n')             #公司介绍
        welfare = pick_xpaths(sel,'//div[@class="d-c-left-ico"]/dl//dd//text()|//div[@class="d-welf-items"]/ul/li/text()',',')[:120]          #福利标签
        item['welfarelabel'] = welfare
        item['origin'] =  'ganji'
        
        #公司信息
        #公司名称
        item['company'] = pick_xpath(sel,'//*[@id="companyName"]/span[1]/a/text()|//*[@id="companyName"]/a/text()')

        #if not item['company']:
        #    item['company'] = pick_xpath(sel,'//*[@class="firm-name"]/a/text()')
        tmp = u"公司行业"
        item['industry'] = pick_xpath(sel,'//div[@class="detail-r-companyInfo"]/div[contains(text(),"%s")]//a/text()'%tmp) #公司行业
        tmp = u"公司性质"
        item['comprop'] = pick_xpath(sel,'//div[@class="detail-r-companyInfo"]/div[contains(text(),"%s")]//a/text()'%tmp) #公司性质
        tmp = u"公司规模"
        item['scale'] = pick_xpath(sel,'//div[@class="detail-r-companyInfo"]/div[contains(text(),"%s")]/span/text()'%tmp) #公司规模       

        fullphone = pick_xpath(sel,'//div[@class="l-d-con"][1]/div[1]/@data-pub-resume-url') 
        if job_type==3 and not fullphone:
            item['contact_name'] = ''
            item['contact']      = ''
             
            log.msg('No full phone',_level='DEBUG')
        elif job_type ==3:
            yield Request(fullphone,meta={'item':item},callback=self.parse_allphone)
        else:
            #兼职
            item['contact_name'] = pick_xpath(sel,'//*[@id="isShowPhoneTop"]/em/text()')
            item['contact'] = pick_xpath(sel,'//*[@id="isShowPhoneTop"]/img/@src')
            if not item['contact'].startswith('http'):
                 item['contact'] = cityurl + item['contact']

        companyurl = pick_xpath(sel,'//span[@class="firm-name"]/a/@href')
        if companyurl:
            yield Request(companyurl,meta={'item':item},callback=self.parse_companyurl)
        else:
            item['shopurl'] = ''

        if 'contact' in item and 'shopurl' in item:
            yield item

    
    def parse_companyurl(self,response):
        sel = Selector(response)
        item = response.meta['item']
        tmp = u'网站'
        shopurl = pick_xpath(sel,'//div[@class="c-introduce"]//em[contains(text(),"%s")]/following-sibling::text()[1]'%tmp).strip('"')
        item['shopurl'] = shopurl
        #log.msg('#@'*30+shopurl)
        tmp = u'地址'
        tmp_str = pick_xpath(sel,'//div[@class="c-introduce"]//em[contains(text(),"%s")]/following-sibling::text()[1]'%tmp).strip('"')
        item['shopaddr'] = tmp_str
        if 'contact' in item:
            yield item
    
    def parse_allphone(self,response):
        sel = Selector(response)
        item = response.meta['item']
        #联系方式和联系人
        item['contact_name'] = pick_xpath(sel,'//*[@id="simple_resume_base_field"]/div[1]/span[2]/text()') 
        item['contact'] = pick_xpath(sel,'//*[@id="simple_resume_base_field"]/div[1]/b/text()')
        #log.msg("Job item :"+item['name']+item['contact']+item['contact_name'],_level='DEBUG')

        if 'shopurl' in item:
            yield item 
        #scrapy runspider jobganji.py -a para=雅安 -o tst2.json
