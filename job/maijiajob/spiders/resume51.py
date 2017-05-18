# -*- coding: utf-8 -*-
__author__ = 'BoDing'

import scrapy


class Resume51Spider(scrapy.Spider):
    name = "resume51"

    start_urls = (
        'http://ehire.51job.com/MainLogin.aspx',
    )

    def parse(self, response):
        return [scrapy.FormRequest(
            url='https://ehirelogin.51job.com/Member/UserLogin.aspx',
            formdata={'ctmName': "光云软件",
                      'userName': 'raycloud',
                      'password': 'tgh6893206',
                      'ec': '510079d7f2b64505a97ecb2beadc7e18',
                      'sc': '99c5e3b62e3bf89a'
            },
            callback=self.after_login
        )]

    def after_login(self, response):
        """登陆失败，尝试使用selenium"""