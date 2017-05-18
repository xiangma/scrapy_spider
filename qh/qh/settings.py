# -*- coding: utf-8 -*-

# Scrapy settings for qh project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'qh'

SPIDER_MODULES = ['qh.spiders']
NEWSPIDER_MODULE = 'qh.spiders'
ITEM_PIPELINES = {'qh.pipelines.QhPipeline':100}
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'qh (+http://www.yourdomain.com)'
