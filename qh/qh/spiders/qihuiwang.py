# -*- coding: utf-8 -*-
import scrapy
import os
from scrapy.http import Request
from qh.items import QhItem

class QihuiwangSpider(scrapy.Spider):
    name = "qihuiwang"
    allowed_domains = ["www.qihuiwang.com"]
    start_urls = ['http://www.qihuiwang.com']
    def parse(self, response):
        for k in range(1,101):
            str_k = unicode(str(k),"utf-8")
            page_urls = u'http://www.qihuiwang.com/productList/q00/p'+str_k+u'/?search=电源适配器'
            yield Request(url = page_urls, callback = self.parse_sickinfo )
    def parse_sickinfo(self,response):
        lis = response.xpath('//ul[@class="proList02"]/li')
#         print('------------------------')
#         print(len(lis))
#         item = QhItem()
        for li in lis:
            item = QhItem()
            item['product'] = li.xpath('./a[@class="proName02"]/text()').extract()[0]
            item['company'] = li.xpath('./p[4]/a/text()').extract()[0]
            companyherf = 'http://www.qihuiwang.com'+li.xpath('./p[4]/a/@href').extract()[0]
            item['uptime'] = li.xpath('./p[2]/text()').extract()[0]
            item['qq'] = li.xpath('./div[@class="contactUs"]/a[@class="qqBtn"]/@qq').extract()[0]
            yield Request(url = companyherf,meta={'item': item},callback = self.parse_detalinfo )
    def parse_detalinfo(self,response):
        item = response.meta['item']
        item['city'] = response.xpath('//div[@class="aiMain"]/ul/li[@class="last"]/text()').extract()[0]
        item['name'] = response.xpath('//div[@class="authInfo"]/div[@class="aiMain"]/ul/li[3]/text()').extract()[0]
        yield item