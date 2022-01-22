# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from .user_agents import random_user_agent

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
import base64

class RandomUserAgentMiddleware(object):
    def process_request(self, request, spider):
        ua = random_user_agent()
        if ua:
            request.headers.setdefault('User-Agent', ua)


class PythonSpidersSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class PythonSpidersDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

    
class ProxyMiddleware(object):
    def __init__(self, settings):
        super(ProxyMiddleware, self).__init__()
        self.proxy_on = settings.get("PROXY_ON")
        self.proxy_tr_on = settings.get("PROXY_TR_ON")
        self.proxy_fr_on = settings.get("PROXY_FR_ON")
        self.proxy_us_on = settings.get("PROXY_US_ON")
        self.proxy_uk_on = settings.get("PROXY_UK_ON")
        self.proxy_au_on = settings.get("PROXY_AU_ON")

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        return obj

    def process_request(self, request, spider):
        
        user = 'hl_64465eb5' #'hl_48a6b2fc'
        pasw = '9051o9yyg5pr' #6tz04qdhe2e7'

        if self.proxy_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-de:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass
            
        if self.proxy_tr_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-tr:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass
            
        if self.proxy_fr_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-fr:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass
            
        if self.proxy_us_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-us:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass
            
        if self.proxy_uk_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-uk:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass
        
        if self.proxy_au_on:
            request.meta["proxy"] = "http://zproxy.lum-superproxy.io:22225"
            proxy_user_pass = f"lum-customer-{user}-zone-z-country-au:{pasw}"
            encoded_user_pass = base64.encodestring(proxy_user_pass.encode()).decode()
            request.headers["Proxy-Authorization"] = "Basic " + encoded_user_pass

