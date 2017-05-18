# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
# __author__ = 'BoDing'

# import hashlib
#import MySQLdb
# from scrapy import log
from openpyxl import Workbook
# import re

# taoresume_workexp_dict = {
#     '时间' : 'time_range',
#     '行业' : 'industry',
#     '公司名称' : 'company',
#     '公司性质' : 'nature',
#     '公司规模' : 'scale',
#     '职责描述' : 'duty',
#     '业绩描述' : 'performance'
# }
# taoresume_workexpds_dict = {
#     '时间' : 'time_range',
#     '电商平台' : 'platform',
#     '工作内容' : 'work_brief',
#     '工作描述' : 'work_detail'
# }
# taoresume_eduexp_dict = {
#     '时间' : 'time_range',
#     '学校' : 'school',
#     '专业' : 'major',
#     '学历' : 'degree',
#     '表现' : 'performance'
# }

class Joblagou(object):
    wb = Workbook()
    ws = wb.active
    ws.append(['所在城市','公司主页','类型','公司logo','公司名称','公司介绍','部门','规模','地址','职位','薪资','工作经验','学历','职位性质','职位诱惑','公司类型','职位描述','发布时间','来源'])
    def process_item(self, item, spider):
        print("!!!!!!!!!!!!!!!!!---this is pipline--!!!!!!!!!!!!!!!!!!!!!!!!!")
        name = spider.name
        if name == "joblagou":
            line = [item['city'].encode("utf8"),item['shopurl'].encode("utf8"),item['industry'].encode("utf8"),item['logo'].encode("utf8"),item['company'].encode("utf8"),item['comprofile'].encode("utf8"),item['depart'].encode("utf8"),item['scale'].encode("utf8"),item['shopaddr'].encode("utf8"),item['name'].encode("utf8"),item['salary'].encode("utf8"),item['experience'].encode("utf8"),item['education'].encode("utf8"),item['prop'].encode("utf8"),item['lure'].encode("utf8"),item['nature'].encode("utf8"),item['desc'].encode("utf8"),item['publish'].encode("utf8"),item['origin'].encode("utf8")]
            self.ws.append(line)
            self.wb.save('LagouJobs.xlsx')
            return item








# class DataCleaner(object):

#     def process_item(self, item, spider):
#         """
#         对所有字段的值做trim，
#         如果是list, 则用逗号join成一个字符串
#         """
#         name = spider.name

#         def clean(x):
#             if x and not isinstance(x, list):
#                 return x.strip()
#             elif x and isinstance(x, list):
#                 x = map(lambda s: s.strip(), x)
#                 return "".join(x)
#             else:
#                 return None

#         if name == "job58":
#             for key in item.keys():
#                 item[key] = clean(item[key])
#             item["origin"] = "58同城"

#         elif name == "resume58":
#             for key in item.keys():
#                 item[key] = clean((item[key]))
#             # 专门处理期望薪资中的空格
#             if item["exp_salary"]:
#                 item["exp_salary"] = item["exp_salary"].split(' ')[-1]
#             item["origin"] = "58同城"

#         elif name == "taojob":
#             for key in item.keys():
#                 item[key] = clean(item[key])
#             # 处理address 中的空白字符
#             if "address" in item:
#                 item["address"] = re.sub(r'[\t\n\s]+', ',', item["address"])
#             if "shopaddr" in item:
#                 item["shopaddr"] = re.sub(r'[\t\n\s]+', '', item["shopaddr"])
#             item["origin"] = "淘工作"

#         elif name == "taoresume":
#             for key in item.keys():
#                 item[key] = clean(item[key])
#             item["origin"] = "淘工作"
#         return item


# import json
# from scrapy import selector


# class BigFieldsPipeline(object):
#     """将大字段转换成JSON格式"""
#     def process_item(self, item, spider):
#         if "resume58" == spider.name:
#             if "work_exp" in item and item["work_exp"]:
#                 sel = selector.Selector(text=item["work_exp"])
#                 infoviews = sel.xpath('//div[contains(@class,"infoview")]')
#                 dic_list = map(self.parse_work_exp_58, infoviews)
#                 item["work_exp"] = json.dumps(dic_list, ensure_ascii=False)
#             if "edu_exp" in item and item["edu_exp"]:
#                 sel = selector.Selector(text=item["edu_exp"])
#                 infoviews = sel.xpath('//div[contains(@class,"infoview")]')
#                 dic_list = map(self.parse_edu_exp_58, infoviews)
#                 item["edu_exp"] = json.dumps(dic_list, ensure_ascii=False)
#             if "lang_skill" in item and item["lang_skill"]:
#                 sel = selector.Selector(text=item["lang_skill"])
#                 ps = sel.xpath('//div[contains(@class,"infoview")]/p')  # 只有一个infoview, 一个p标签对应一个语言
#                 dic_list = map(self.parse_lang_alility_58, ps)
#                 item["lang_skill"] = json.dumps(dic_list, ensure_ascii=False)
#             if "cert" in item and item["cert"]:
#                 sel = selector.Selector(text=item["cert"])
#                 ps = sel.xpath('//div[contains(@class,"infoview")]/p')  # 只有一个infoview, 一个p标签对应一个证书
#                 dic_list = map(self.parse_cert_58, ps)
#                 item["cert"] = json.dumps(dic_list, ensure_ascii=False)
#             if "ability" in item and item["ability"]:
#                 sel = selector.Selector(text=item["ability"])
#                 ps = sel.xpath('//div/p')
#                 ab_list = map(self.parse_ability_58, ps)  # 一项技能拼成一行，比如 Photoshop：一般3年经验
#                 item["ability"] = '\n'.join(ab_list)
#             if "showme" in item and item["showme"]:
#                 sel = selector.Selector(text=item["showme"])
#                 pic_list = sel.xpath('//div[contains(@class,"infoview")]/ul[@class="myphoto"]/li/img/@_src').extract()   # 只有一个infoview
#                 item["showme"] = json.dumps(pic_list, ensure_ascii=False)

#         if "taoresume" == spider.name:
#             if "work_exp" in item and item["work_exp"]:
#                 # 将html形式的数据，转化为(key,value)列表
#                 sel = selector.Selector(text=item["work_exp"])
#                 fields = sel.xpath('//div[@class="x-formfield"]')
#                 seg_list = map(self.parse_work_exp_seg, fields)
#                 # 将list拆分成对象
#                 exp_list = []
#                 exp = None
#                 for seg in seg_list:
#                     if seg[0] == u"时间":  # 以时间作为一个工作经验开始的标志
#                         exp = dict()
#                         exp[taoresume_workexp_dict[seg[0]]] = seg[1]
#                         exp_list.append(exp)
#                     else:
#                         exp[taoresume_workexp_dict[seg[0]]] = seg[1]
#                 item["work_exp"] = json.dumps(exp_list, ensure_ascii=False)
#             else:
#                 item["work_exp"] = None

#             if "work_exp_ds" in item and item["work_exp_ds"]:
#                 # 将html转化为(key,value)列表
#                 sel = selector.Selector(text=item["work_exp_ds"])
#                 fields = sel.xpath('//div[@class="x-formfield"]')
#                 seg_list = map(self.parse_work_exp_seg, fields)
#                 exp_list = []
#                 for seg in seg_list:
#                     if seg[0] == u"时间":  # 以时间作为一个工作经验开始的标志
#                         exp = dict()
#                         exp[taoresume_workexpds_dict[seg[0]]] = seg[1]
#                         exp_list.append(exp)
#                     else:
#                         exp[taoresume_workexpds_dict[seg[0]]] = seg[1]
#                 item["work_exp_ds"] = json.dumps(exp_list, ensure_ascii=False)
#             else:
#                 item["work_exp_ds"] = None

#             if "edu_exp" in item and item["edu_exp"]:
#                 sel = selector.Selector(text=item["edu_exp"])
#                 d = dict()
#                 d["time_range"] = sel.xpath('//div[@class="x-formfield"][1]/div/text()').extract()[0].strip()
#                 d["school"] = sel.xpath('//div[@class="x-formfield"][2]/div/text()').extract()[0].strip()
#                 d["degree"] = sel.xpath('//div[@class="x-formfield"][3]/div/text()').extract()[0].strip()
#                 major = sel.xpath('//div[@class="x-formfield"][4]/div/text()').extract()
#                 if major:
#                     d["major"] = major[0].strip()
#                 performance = sel.xpath('//div[@class="x-formfield"][5]/div/text()').extract()
#                 if performance:
#                     d['performance'] = performance[0].strip()
#                 item["edu_exp"] = json.dumps(d, ensure_ascii=False)
#             else:
#                 item["edu_exp"] = None

#         return item


#     def parse_work_exp_58(self, sel):
#         d = {}
#         # 公司名称
#         company = sel.xpath('h4/text()').extract()[0]
#         d['company'] = company
#         # 在职时间
#         time_range = sel.xpath('p[1]/span[2]/text()').extract()[0].strip()
#         d["time_range"] = time_range
#         # 薪资水平
#         salary_level = sel.xpath('p[2]/span[2]/text()').extract()[0].strip()
#         d['salary_level'] = salary_level
#         # 职位名称
#         position = sel.xpath('p[3]/span[2]/text()').extract()[0].strip()
#         d['position'] = position
#         # 工作内容
#         duty = sel.xpath('p[4]/span[2]/text()').extract()[0].strip()
#         d['duty'] = duty
#         return d

#     def parse_edu_exp_58(self, sel):
#         d = {}
#         # 在校时间
#         d['time_range'] = sel.xpath('ul/li[1]/text()').extract()[0]
#         # 学校名称
#         d['school'] = sel.xpath('ul/li[2]/text()').extract()[0]
#         # 专业
#         d['major'] = sel.xpath('ul/li[3]/text()').extract()[0]
#         return d

#     def parse_lang_alility_58(self, sel):
#         d = {}
#         # 语种
#         d["lang_name"] = sel.xpath('span[1]/text()').extract()[0].strip().strip(u'：')
#         # 听说能力
#         d["ting_shuo"] = sel.xpath('span[2]/span[1]/text()').extract()[0]
#         # 读写能力
#         d["du_xie"] = sel.xpath('span[2]/span[2]/text()').extract()[0]
#         # 证书
#         if len(sel.xpath('span[2]/span')) > 2:
#             d["cert"] = sel.xpath('span[2]/span[3]/text()').extract()[0]
#         return d

#     def parse_cert_58(self, sel):
#         d = {}
#         # 证书名称
#         d['name'] = sel.xpath('span[1]/text()').extract()[0]
#         # 发证时间
#         d['issue_date'] = sel.xpath('span[2]/text()').extract()[0].strip()
#         return d

#     def parse_ability_58(self, sel):
#         """
#         <p class="pst"><span class="sth">Word：</span><span class="std"><span>精通</span><span>11年经验</span></span></p>
#         """
#         skill = sel.xpath('span/text()').extract()[0]
#         level = sel.xpath('span/span/text()').extract()
#         return skill + ''.join(level)

#     def parse_work_exp_seg(self, sel):
#         label = sel.xpath('label/text()').extract()[0].strip().strip(u':')
#         value = sel.xpath('div/text()').extract()[0].strip()
#         return label, value

# class MySQLPipeline(object):

#    def __init__(self):
#        self.host = "localhost"
#        #self.host = "127.0.0.1"
#        self.db = "scrapy"
#        self.user = "root"
#        self.passwd = "mx1993"
#        self.count = 0

#    def process_item(self, item, spider):
#        spider_name = spider.name

#        try:
#            conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, charset='utf8')
#            conn.select_db(self.db)
#            cur = conn.cursor()
#            sql, para = self.construct_sql(spider_name, item)
#            cur.execute(sql, para)
#            self.count += 1
#            log.msg(self.count)
#            conn.commit()
#            cur.close()
#            conn.close()
#        except MySQLdb.Error,e:
#            log.msg("MySQL error:" + str(e), _level=log.ERROR)

#        return item

#    def construct_sql(self, spider_name, item):
#        if spider_name == "job58" or spider_name == "taojob":
#            keys = item.keys()
#            sql = "INSERT into maijia_job_tmp(createtime ,`"
#            sql += "`,`".join(keys) + "`)"
#            sql += " VALUES(CURRENT_TIMESTAMP" + ",%s" * len(keys) + ")"
#            para = [item[key] for key in item.keys()]
#            return sql, para
#        if spider_name == "resume58":
#            keys = item.keys()
#            sql = "INSERT into maijia_resume_tmp(createtime ,`"
#            sql += "`,`".join(keys) + "`)"
#            sql += " VALUES(CURRENT_TIMESTAMP" + ",%s" * len(keys) + ")"
#            para = [item[key] for key in item.keys()]
#            return sql, para
#        if spider_name == "taoresume":
#            keys = item.keys()
#            sql = "INSERT into maijia_resume_tmp(createtime ,`"
#            sql += "`,`".join(keys) + "`)"
#            sql += " VALUES(CURRENT_TIMESTAMP" + ",%s" * len(keys) + ")"
#            para = [item[key] for key in item.keys()t]
#            return sql, para

#        else:
#            raise Exception("No such spider")




