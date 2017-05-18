#-*-coding:utf-8-*-  

import random
import base64
from scrapy.contrib.downloadermiddleware.useragent import UserAgentMiddleware

class RotateUserAgentMiddleware(UserAgentMiddleware):
	def __init__(self, user_agent=''): 
		self.user_agent = user_agent

	def process_request(self, request, spider):
		ua = random.choice(self.user_agent_list)
		if ua:
			print('---!!!******___---useragent is:'+ua),
			request.headers.setdefault('User_Agent', ua)

	user_agent_list = [
       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0',
       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36',
       "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
       "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
       "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
       "Mozilla/5.0 (Windows; U; WinNT4.0; en-US; rv:1.2b) Gecko/20021001 Phoenix/0.2",
       "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.23) Gecko/20090825 SeaMonkey/1.1.18",
       "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/527  (KHTML, like Gecko, Safari/419.3) Arora/0.6 (Change: )",
       "Mozilla/5.0 (Windows; U; ; en-NZ) AppleWebKit/527  (KHTML, like Gecko, Safari/419.3) Arora/0.8.0",
       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Beamrise/17.2.0.9 Chrome/17.0.939.0 Safari/535.8",
       "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/18.6.872.0 Safari/535.2 UNTRUSTED/1.0 3gpp-gba UNTRUSTED/1.0",
       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
       "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:10.0.1) Gecko/20100101 Firefox/10.0.1",
       "Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20120403211507 Firefox/12.0",
       "Mozilla/5.0 (Windows NT 6.0; rv:14.0) Gecko/20100101 Firefox/14.0.1",
       "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:15.0) Gecko/20120427 Firefox/15.0a1",
       "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0) Gecko/16.0 Firefox/16.0",
       "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.1 (KHTML, like Gecko) Maxthon/3.0.8.2 Safari/533.1",
       "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
       "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
       "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)",
       "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/4.0)",
       "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
       "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/5.0)",
       "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
       "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.2; Trident/5.0)",
       "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.2; WOW64; Trident/5.0)",
       "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)",
       "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/6.0)"
       ] 

class RandomProxyMiddleware(object):
       def process_request(self,request,spider):
              proxy = random.choice(self.PROXIES)
              if proxy:
                     request.meta['proxy'] = "http://%s"%(proxy['ip_port'])
                     encoded_user_pass = base64.encodestring(proxy['user_pass'])
                     request.headers['Proxy-Authorization'] = 'basic' + encoded_user_pass
                     print("---!!!----**---proxy ip is:"+request.meta['proxy'])
              else:
                     print("--!!!---***--ProxyMiddleware no pass--!!")
                     request.meta['proxy'] = "http://%s" % proxy['ip_port']

       PROXIES = [
       {'ip_port': '222.80.73.172:8998', 'user_pass': ''},
       {'ip_port': '27.18.113.99:8998', 'user_pass': ''},
       {'ip_port': '27.18.4.249:808', 'user_pass': ''},
       {'ip_port': '27.18.141.237:8998', 'user_pass': ''},
       {'ip_port': '183.32.89.44:808', 'user_pass': ''},
       {'ip_port': '111.23.10.27:80', 'user_pass': ''},
       ]
