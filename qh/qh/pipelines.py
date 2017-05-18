# -*- coding: utf-8 -*-
from openpyxl import Workbook
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class QhPipeline(object):
    wb = Workbook()
    ws = wb.active
    ws.append(['公司名称','法人','公司地址','产品名称','qq','更新时间'])
    def process_item(self, item, spider):
    	name = spider.name
    	if name == "qihuiwang":
	        line = [item['company'].encode("utf8"),item['name'].encode("utf8"),item['city'].encode("utf8"),item['product'].encode("utf8"),item['qq'].encode("utf8"),item['uptime'].encode("utf8")]
	        self.ws.append(line)
	        self.wb.save('qihuiwang.xlsx')
	        return item
