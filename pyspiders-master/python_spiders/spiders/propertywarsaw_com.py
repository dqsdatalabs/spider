# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
class MySpider(Spider):
    name = 'propertywarsaw_com'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source = "Propertywarsaw_PySpider_poland"
    custom_settings = {
    #   "PROXY_TR_ON": True,
      "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
      "HTTPCACHE_ENABLED": False,
      "RETRY_TIMES": 3,
      "DOWNLOAD_DELAY": 3,
      
    }
    handle_httpstatus_list = [403]
    headers={
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.property-warsaw.com/properties/search?page=1&branchId=warsaw&type=apartment&want=rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.property-warsaw.com/properties/search?page=1&branchId=warsaw&type=house&want=rent",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='tabcontent']//div[@class='col-md-7 col-lg-6']"):
            follow_url = item.xpath(".//a/@href").get()
            
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
   
        if page == 2 or seen:
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page": page + 1, "property_type": response.meta.get('property_type')})
   
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)  
        item_loader.add_value("property_type", response.meta.get('property_type'))        
        
        external_id = response.xpath("//div[@class='col-12']//text()[contains(.,'ref:')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("ref:")[-1].replace(")", "").strip())

        title = response.xpath("//div[@class='single-property col-md-8 col-lg-9']/h2/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        
        rent = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'rice')]/following-sibling::div/span/text()").get()
        if rent:
            price = rent.split(" PLN")[0].replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "PLN")
        
        room_count = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'edroom')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'athroom')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'Size')]/following-sibling::div/text()").get()
        if square_meters:
            square_meters = square_meters.replace("m", "")
            item_loader.add_value("square_meters", square_meters)
        
        city = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'City')]/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("city", city)
        
        address = response.xpath("//div[contains(@class,'col-12 widget')]/div[contains(.,'Street')]/following-sibling::div/a/text()").get()
        if address:
            if city:
                address = address + ", " + city
                item_loader.add_value("address", address)
            else:
                item_loader.add_value("address", address)
        else:
            item_loader.add_value("address", city)

        parking = response.xpath("//span/img/@src[contains(.,'arking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//span/img/@src[contains(.,'alcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//span/img/@src[contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool = response.xpath("//span/img/@src[contains(.,'wimming')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        pets_allowed = response.xpath("//span/img/@src[contains(.,'pet')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        latitude = response.xpath("//div[@id='google-map-warsaw']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='google-map-warsaw']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        description = " ".join(response.xpath("//div[@class='col-md-8 col-lg-9']/p/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[@id='links']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Hamilton May Warsaw")
        item_loader.add_value("landlord_phone", "(+48) 22 428 16 15")
        item_loader.add_value("landlord_email", "warsaw@hamiltonmay.com")
        
        yield item_loader.load_item()