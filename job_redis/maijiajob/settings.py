# -*- coding: utf-8 -*-

# Scrapy settings for maijiajob project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'maijiajob'

SPIDER_MODULES = ['maijiajob.spiders']
NEWSPIDER_MODULE = 'maijiajob.spiders'
# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'

# DOWNLOAD_DELAY = 0.1
DOWNLOADER_MIDDLEWARES = {
	'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware':None,
	'maijiajob.middlewares.RotateUserAgentMiddleware':400,
}


#启用Redis调度存储请求队列
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
#确保所有的爬虫通过Redis去重
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderPriorityQueue'
SCHEDULER_PERSIST = True
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

CONCURRENT_REQUESTS = 20 #开启线程数量，默认16 
CONCURRENT_REQUESTS_PER_DOMAIN = 3
COOKIES_DEBUG = False
# COOKIES_ENABLED = False
LOG_LEVEL = "INFO"

ITEM_PIPELINES = {
    'scrapy_redis.pipelines.RedisPipeline': 302,
    'maijiajob.pipelines.Joblagou': 303,
    #'maijiajob.pipelines.MySQLPipeline': 304
}

