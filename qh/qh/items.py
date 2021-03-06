# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class QhItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    product = scrapy.Field()     #产品名称
    company = scrapy.Field()     #公司名称
    uptime = scrapy.Field()      #发布时间
    city = scrapy.Field()        #所在地城市
    name = scrapy.Field()        #联系人
    qq = scrapy.Field()          #联系人qq
    mailbox = scrapy.Field()     #联系人邮箱
    Mphone = scrapy.Field()      #联系人手机
    Clink = scrapy.Field()       #公司网址
