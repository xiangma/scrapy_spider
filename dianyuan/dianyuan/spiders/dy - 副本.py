# -*- coding: utf-8 -*-
import scrapy
import os
from scrapy.http import Request
from dianyuan.items import DianyuanItem
 
class DySpider(scrapy.Spider):
    name = "dy"
    allowed_domains = ["product.dianyuan.com"]
    start_urls = ['http://company.dianyuan.com/index.php?do=biz_product_list&productName=电源适配器&city=&vip=&only=&rcTotal=1714&rcStart=0&rcLimit=1714']
#    url_list = set()
    def parse(self, response):
        movies = response.xpath('//div[@class="list"]/table/tr')
        for k in range(1,len(movies)):
#for each_movie in movies:
#            item = DianyuanItem()
#            item['product'] = movies[k].xpath('./td[@class="l cpy"]/h3/a/@title').extract()[0]
#            item['addr'] = movies[k].xpath('./td[@class="l cpy"]/h3/a/@href').extract()[0]
             urlst = movies[k].xpath('./td[@class="l cpy"]/h3/a/@href').extract()[0]
#            item['company'] = movies[k].xpath('./td[@class="l cpy"]/p[@class="cNm"]/a/@title').extract()[0]
#            item['uptime'] = movies[k].xpath('./td[@class="l cpy"]/p[@class="pTm"]/span').xpath('string(.)').extract()[0]
#            yield item
             yield Request (url=urlst,callback = self.parse_sickinfo)
    def parse_sickinfo(self,response):
        trs = response.xpath('//div[@class="cpyContact mCommStyle"]/table/tbody/tr')
        item = DianyuanItem()
        item['name'] = trs[0].xpath('./td').xpath('string(.)').extract()[0]
        item['phone'] = trs[1].xpath('./td').xpath('string(.)').extract()[0]
        item['mailbox'] = trs[2].xpath('./td/a').xpath('string(.)').extract()[0]
        item['Mphone'] = trs[4].xpath('./td').xpath('string(.)').extract()[0]  
        item['Clink'] = trs[6].xpath('./td/a/@href').xpath('string(.)').extract()[0]  
        yield item        