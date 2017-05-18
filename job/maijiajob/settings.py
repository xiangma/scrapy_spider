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

DOWNLOAD_DELAY = 0.1
DOWNLOADER_MIDDLEWARES = {
	'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware':None,
	'maijiajob.middlewares.RotateUserAgentMiddleware':400,
	# 'maijiajob.middlewares.RandomProxyMiddleware':401,
}
FEED_URI = "items.json"
FEED_FORMAT = "json"
CONCURRENT_REQUESTS = 3
CONCURRENT_REQUESTS_PER_DOMAIN = 3
COOKIES_DEBUG = False
# COOKIES_ENABLED = False
LOG_LEVEL = "INFO"

ITEM_PIPELINES = {'maijiajob.pipelines.Joblagou': 303}

