# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join


class LagouItem(Item):
    """
    拉钩要抓取的职位信息
    """
    city = scrapy.Field()  # 搜索城市
    url = scrapy.Field()  # 原始url

    # 职位基础信息部分
    category = scrapy.Field()  # 职位类别
    name = scrapy.Field()  # 职位名称
    depart = scrapy.Field()  # 所属部门
    # address = scrapy.Field()  # 工作地址
    salary = scrapy.Field()  # 薪资
    experience = scrapy.Field()  # 工作经验
    education = scrapy.Field()  # 学历要求
    prop = scrapy.Field()  # 职位性质  兼职  实习 全职
    lure = scrapy.Field()  # 职位诱惑
    # welfarelabel = scrapy.Field()  # 福利标签
    publish = scrapy.Field()  # 发布时间或更新时间
    workhour = scrapy.Field()  # 工作时间
    gender = scrapy.Field()  # 性别要求
    comment = scrapy.Field()  # 评论
    contact = scrapy.Field()  # 联系人 电话
    contact_name = scrapy.Field() #联系人

    # 职位描述
    desc = scrapy.Field()

    # 公司信息
    nature = scrapy.Field()  # 公司性质
    logo = scrapy.Field()  # 店铺logo
    company = scrapy.Field()  # 公司名称
    shop = scrapy.Field()  # 店铺名称
    industry = scrapy.Field()  # 行业
    scale = scrapy.Field()  # 公司规模
    shopurl = scrapy.Field()  # 公司主页
    shopaddr = scrapy.Field()  # 店铺详细地址
    # comprop = scrapy.Field()  # 公司性质
    comprofile = scrapy.Field()  # 公司简介
    compics = scrapy.Field()  # 公司图片
    sales = scrapy.Field()  # 销售额
    comcom = scrapy.Field()  # 评论
    welfare = scrapy.Field()  # 公司福利
    story = scrapy.Field()  # 公司报道
    mailbox = scrapy.Field()  # 公司邮箱

    # 附加信息
    date = scrapy.Field()    # 抓取信息的时间
    origin = scrapy.Field()  # 来源网站

class LagouLoader(ItemLoader):
    default_item_class = LagouItem
    default_input_processor = MapCompose(lambda s: s.strip())
    default_output_processor = TakeFirst()
    description_out = Join()