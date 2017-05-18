# -*- coding: utf-8 -*-
from openpyxl import Workbook
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class DianyuanPipeline(object):
    wb = Workbook()
    ws = wb.active
    ws.append(['公司名称','产品名称','所在城市','联系人姓名','移动电话','座机','邮箱','公司网址','更新时间'])
       
    def process_item(self, item, spider):
#         with open("dianyuanwan.txt",'a') as fp:
            line = [item['company'].encode("utf8"),item['product'].encode("utf8"),item['city'].encode("utf8"),item['name'].encode("utf8"),item['Mphone'].encode("utf8"),item['phone'].encode("utf8"),item['mailbox'].encode("utf8"),item['Clink'].encode("utf8"),item['uptime'].encode("utf8")]
            self.ws.append(line)
            self.wb.save('dianyuanwan.xlsx')
            return item
#             fp.write('产品名称:' + item['product'].encode("utf8") + '   ')
#             fp.write('公司名称:' + item['company'].encode("utf8") + '   ')
#             fp.write('更新时间:' + item['uptime'].encode("utf8") + '   ')
#             fp.write('所在城市:' + item['city'].encode("utf8") + '   ')
#             fp.write('联系人姓名:' + item['name'].encode("utf8") + '   ')
#             fp.write('座机:' + item['phone'].encode("utf8") + '   ')
#             fp.write('邮箱:' + item['mailbox'].encode("utf8") + '   ')
#             fp.write('移动电话:' + item['Mphone'].encode("utf8") + '   ')
#             fp.write('公司网址:' + item['Clink'].encode("utf8") + '\n')