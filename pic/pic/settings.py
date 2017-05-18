# -*- coding: utf-8 -*-

# Scrapy settings for pic project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'pic'

SPIDER_MODULES = ['pic.spiders']
NEWSPIDER_MODULE = 'pic.spiders'
ITEM_PIPELINES = {'pic.pipelines.PicPipeline':100}
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'pic (+http://www.yourdomain.com)'
