# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import log
from maijiajob.items import ResumeganjiItem
import datetime,re,sys
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import os,urllib,json
from scrapy.core.downloader import  Slot
from kafka import KafkaConsumer, KafkaProducer


reload(sys)
sys.setdefaultencoding('utf-8')

work_exp_dict = {
    u'工作时间' : 'time_range',
    u'职位名称' : 'position',
    u'公司行业' : 'industry',
    u'工作内容' : 'duty',
}
cert_dict = {
    u'证书名称' : 'name',
    u'颁发时间' : 'issue_date',
    u'颁发机构' : 'issue_ca'
}
category_url = {
    '淘宝客服' : 'qztaobaokefu/',      #淘宝客服
    '淘宝美工' : 'qztaobaomeigong/',   #淘宝美工
    #营销推广
    '淘宝推广' : 'qztaobao/_%E6%B7%98%E5%AE%9D%E6%8E%A8%E5%B9%BF/qiuzhi/', #淘宝推广
    '天猫推广' : 'qztaobao/_%E5%A4%A9%E7%8C%AB%E6%8E%A8%E5%B9%BF/qiuzhi/', #天猫推广
    '直通车手' : 'qztaobao/_%E7%9B%B4%E9%80%9A%E8%BD%A6/qiuzhi/', #直通车手
    '网店推广' : 'qztaobao/_%E7%BD%91%E5%BA%97%E6%8E%A8%E5%B9%BF/qiuzhi/', #网店推广
    'SEO'      : 'qztaobao/_SEO/qiuzhi/', # 网站推广/SEO
    '网络营销' : 'qztaobao/_%E7%BD%91%E7%BB%9C%E8%90%A5%E9%94%80/qiuzhi/' , #网络营销
    #店铺管理
    '店铺管理' : 'qzdianpuguanli/', #店铺管理
    '店铺运营' : 'qzdianpuyunying/', #店铺运营
    '淘宝店长' : 'qztaobao/_%E6%B7%98%E5%AE%9D%E5%BA%97%E9%95%BF/qiuzhi/', #淘宝店长
    '天猫店长' : 'qztaobao/_%E5%A4%A9%E7%8C%AB%E5%BA%97%E9%95%BF/qiuzhi/', #天猫店长
    '运营专员' : 'qztaobao/_%E8%BF%90%E8%90%A5%E4%B8%93%E5%91%98/qiuzhi/', #运营专员
    '网店运营' : 'qztaobao/_%E7%BD%91%E5%BA%97%E8%BF%90%E8%90%A5/qiuzhi/', #网店运营
    '电商运营' : 'qztaobao/_%E7%94%B5%E5%95%86%E8%BF%90%E8%90%A5/qiuzhi/', #电商运营
    '运营总监' : 'qztaobao/_%E8%BF%90%E8%90%A5%E6%80%BB%E7%9B%91/qiuzhi/', #运营总监
    '运营主管' : 'qztaobao/_%E8%BF%90%E8%90%A5%E4%B8%BB%E7%AE%A1/qiuzhi/', #运营主管
    '仓库管理' : 'qzcangkuguanli/',
    '快递员'   : 'qzkuaidi/' 
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

class ResumeganjiSpider(Spider):
    name = "resumeganji"
    download_delay = 2
    concurrent_requests = 3
    allowed_domains = ["ganji.com"]
    start_urls = (
        'http://www.ganji.com/index.htm',
    )
    para = ''
    def __init__(self,para=None,para2=None,*args,**kwargs):
        super(ResumeganjiSpider, self).__init__(*args,**kwargs)
        if para:
            #self.para = para.decode(sys.getdefaultencoding())
            self.para = urllib.unquote(para)
            log.msg('self para '+ self.para,_level='INFO')
        dispatcher.connect(self.stats_spider_closed, signal=signals.stats_spider_closed)
        dispatcher.connect(self.engine_opened, signal=signals.engine_started)

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
        """可以通过参数给需要爬取的城市，城市间以逗号分隔，不给参数就默认获取所有城市"""
        sel = Selector(response)
        allcities = sel.xpath('/html/body/div[1]/div[3]//a')

        if self.para:
            para_cities = self.para.split(u',')
            #print '-'*30,self.para,para_cities,type(para_cities)
            for para_city in para_cities:
                #print '-'*30,'para_city',para_city,type(para_city)#,para_city.encode('utf-8')
                cityname = para_city.strip()
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
        """对每个城市的那几类简历进行分别爬取"""
        sel = Selector(response)
        cityurl = response.meta['cityurl']
        cityname = response.meta['cityname']
        for key in category_url.iterkeys():
            category = key
            cate_url = category_url.get(key)
            log.msg('cityurl+cateurl'+'-'*30+cityurl+cate_url,_level='DEBUG')
            yield Request(cityurl+cate_url,meta={'category':category,'cityurl':cityurl,'cityname':cityname},callback=self.parse_resume_list)

    def parse_resume_list(self,response):
        """每一类简历的查询结果"""
        sel = Selector(response)
        cityurl = response.meta['cityurl']
        cityname = response.meta['cityname']
        category = response.meta['category']

        resume_list = sel.xpath('//div[@class="qz-resume-list"]//dl')
        dd = datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)
        yesterday_date = "%02d-%02d" % (dd.month, dd.day)

        dd = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        today = "%02d-%02d" % (dd.month, dd.day)

        for resume in resume_list:
            """赶集默认按时间排序，所以遇到第一个不满足的即可退出"""
            refresh_date = resume.xpath('dd/text()')[-1].extract().strip() #更新时间

            log.msg('refresh_date before re:-----------'+ str(refresh_date),_level='DEBUG')
            
            #测试专用
            #yield Request('http://dazhou.ganji.com//jianli/773725114x.htm',meta={'category':category,'cityname':cityname},callback=self.parse_resume)
            #return
            #测试专用

            refresh_date = re.findall(r'(\d{2}-\d{2})',refresh_date)
            log.msg('yesterday_date:'+ str(yesterday_date) + '-'*30 + 'refresh_date:'+ str(refresh_date),_level='DEBUG')

            if  not refresh_date or (refresh_date[0] >= yesterday_date and refresh_date[0] <= today):
                resume_url =  pick_xpath(resume,'.//a[contains(@href,"jianli")]/@href')
                log.msg('Request resume url : '+ cityurl+resume_url,_level='DEBUG')
                yield Request(cityurl+resume_url,meta={'category':category,'cityname':cityname},callback=self.parse_resume)
            else:
                log.msg('spider stop'+ '-'*30 + 'refresh_date:'+ str(refresh_date),_level='DEBUG')

                return
            # 需要分页
        next_page = pick_xpath(sel,'//div[@class="pageBox"]//a[@class="next"]/@href')  # 判断下一页链接是否存在
        if next_page:
            yield Request(cityurl+next_page,meta={'category':category,'cityurl':cityurl,'cityname':cityname},callback=self.parse_resume_list)


    def parse_resume(self,response):
        """获取每一份简历"""
        sel = Selector(response)
        item = ResumeganjiItem()
        item["url"] = response.url
        item["city"] = response.meta["cityname"]
        item["category"] = response.meta["category"]
        item['name'] = pick_xpath(sel,'//*[@class="offer_name"]/text()')
        sex_age = pick_xpath(sel,'//*[@class="offer_age"]/text()')   # （男，21岁）
        sex_age = sex_age.encode('utf8')
        sex = '男' if sex_age.count('男'.encode('utf-8'))  else '女'
        age = re.findall(r'\d+', sex_age)[0]

        item['gender'] = sex
        item['age'] = age
        item['title'] = pick_xpath(sel,'//*[@class="offer_tit"]/text()')
        resume_detail = sel.xpath('//*[@id="js_contact_container"]')

        tmp = u"期望职位"
        item['exp_pos'] = pick_xpaths(resume_detail,'./li[contains(text(),"%s")]/span//text()'%tmp,' ')

        tmp = u'最高学历'
        item['degree'] = pick_xpath(resume_detail,'./li[contains(text(),"%s")]/span/text()'%tmp)

        tmp = u'期望月薪'
        item['exp_salary'] = pick_xpath(resume_detail,'./li[contains(text(),"%s")]/span/text()'%tmp)

        tmp = u'工作年限'
        item['work_years'] = pick_xpath(resume_detail,'./li[contains(text(),"%s")]/span/text()'%tmp)

        tmp = u'期望地区'
        item['exp_city'] = pick_xpath(resume_detail,'./li[contains(text(),"%s")]/span/text()'%tmp)

        tmp = u"籍"
        item['hometown'] = pick_xpath(resume_detail,'./li[contains(text(),"%s")]/span/text()'%tmp)
        if not item['hometown']:
            hometown =  pick_xpath(sel,'//*[@id="js_contact_container"]/li[contains(text(),"%s")]'%u"籍　　贯")
            if hometown:
                item['hometown'] = hometown[hometown.find('籍　　贯：') + len('籍　　贯：')].strip() #籍贯
            else:
                item['hometown'] = '' 
           
        #work_exp_bref = pick_xpath(sel,('//*[@class="divide_tit" and contains(text(),"%s")]/span/text()'%u"工作经验")) #工作经验简述

        work_exp_nodes = sel.xpath('//*[@class="nrcon workexp"]')  #工作经历
        #work_exp_dict = {}
        #work_exp_dict[u'工作经历简述'] = work_exp_bref

        tmplist = []
        for work_exp in work_exp_nodes:
            pnodes = work_exp.xpath('./p')
            tmp_dict = {}
            for pnode in pnodes:
                tmptxt = pick_xpaths(pnode,'.//text()',' ')
                tmptxt = tmptxt.split(u'：',1)
                if len(tmptxt) == 1:
                    tmp_dict['company'] = tmptxt[0] #公司名称
                else:
                    tmp_dict[work_exp_dict[tmptxt[0].strip()]] = tmptxt[1]
            tmplist.append(tmp_dict)

        #work_exp_dict[u'工作经历'] = tmplist
        item['work_exp'] = json.dumps(tmplist,ensure_ascii=False)

        tmplist = []
        edu_node = sel.xpath('//div[@class="divide_tit" and contains(text(),"%s")]/following-sibling::div[1]'%u"教育经历")
        if edu_node:
            edu_times = edu_node.xpath('./p')               #教育经历

            for pnode in edu_times:
                tmp_dict = {}
                tmptxt = pick_xpaths(pnode,'./text()','#')
                tmptxt = tmptxt.split('#')
                if len(tmptxt) == 4:
                    tmp_dict['time_range'] = tmptxt[0]
                    tmp_dict['school'] = tmptxt[1]
                    tmp_dict['degree'] = tmptxt[2]
                    tmp_dict['major'] = tmptxt[3]
                elif len(tmptxt) == 3:
                    tmp_dict['time_range'] = tmptxt[0]
                    tmp_dict['school'] = tmptxt[1]
                    tmp_dict['degree'] = tmptxt[2]    
                elif len(tmptxt) == 2:
                    tmp_dict['time_range'] = tmptxt[0]
                    tmp_dict['school'] = tmptxt[1]
                tmplist.append(tmp_dict)
            item['edu_exp'] = json.dumps(tmplist,ensure_ascii=False)
        else:
            item['edu_exp'] = ''

        tmplist = []
        lang_skillnode = sel.xpath('//div[@class="divide_tit" and contains(text(),"%s")]/following-sibling::div[1]'%u"语言技能")
        if lang_skillnode:
            lang_skills = lang_skillnode.xpath('./p')#pick_xpaths(lang_skillnode,'text()',' ')               #语言技能

            for pnode in lang_skills:
                tmp_dict = {}
                tmptxt = pick_xpath(pnode,'./text()')
                tmptxt = tmptxt.split(u'：',1)
                tmp_dict['lang_name'] = tmptxt[0]

                if len(tmptxt) == 2:
                    tmp_dict['du_xie'] = tmptxt[1]
                    tmp_dict['ting_shuo'] = tmptxt[1]
                else:
                    tmp_dict['du_xie'] = u'一般'
                    tmp_dict['ting_shuo'] = u'一般'
                tmplist.append(tmp_dict)
            item['lang_skill'] = json.dumps(tmplist,ensure_ascii=False)
        else:
            item['lang_skill'] = ''

        tmplist = []
        edu_node = sel.xpath('//div[@class="divide_tit" and contains(text(),"%s")]/following-sibling::div[1]'%u"证书奖项")
        if edu_node:
            edu_times = edu_node.xpath('./p')               #证书奖项

            for pnode in edu_times:
                tmp_dict = {}
                tmptxt = pick_xpaths(pnode,'./text()','#')
                tmptxt = tmptxt.split('#')
                for tmp in tmptxt:
                    tp = tmp.split(u'：',1)
                    if len(tp) == 2:
                        tmp_dict[cert_dict[tp[0].strip()]] = tp[1].strip()
                tmplist.append(tmp_dict)

            item['cert'] = json.dumps(tmplist,ensure_ascii=False)
        else:
            item['cert'] = ''

        abilitynode = sel.xpath('//div[@class="divide_tit" and contains(text(),"%s")]/following-sibling::div[1]'%u"专业技能")
        if abilitynode:
            ability = pick_xpaths(abilitynode,'text()',' ')               #专业技能
            item['ability'] = ability
        else:
            item['ability'] = ''

        self_desc = pick_xpaths(sel,'//*[@class="nrcon lht24"]//text()','\n')   #自我描述
        if self_desc:
            item['self_intro'] = self_desc
        else:
            item['self_intro'] = ''

        refresh_date = pick_xpaths(sel,'//*[@class="offerxx"]//*[contains(text(),"%s")]/text()'%u"更新时间")

        log.msg('refresh_date in resume :'+'#'*30+refresh_date,_level='DEBUG')
        refresh_date = refresh_date.encode('utf-8')
        refresh_date = refresh_date[refresh_date.find('更新时间：')+len('更新时间：'):].strip()             #更新时间
        item['refresh_time'] = refresh_date
        item['origin'] =  'ganji'

        log.msg(item['title']+item['refresh_time'],_level='DEBUG')
        return item

#scrapy runspider resumeganji.py -a para=雅安 -o tst2.json
