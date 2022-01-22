# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import dateparser
import re

class MySpider(Spider):
    name = 'allresidentialrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Allresidentialrealestate_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://allresidentialrealestate.com.au/renting/properties-for-rent"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='listingImage']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get() 
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//span[@class='ldAddress']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//span[@class='displayPrice']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("per")[0])
        item_loader.add_value("currency","USD")
        images=[x for x in response.xpath("//img[@u='thumb']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//div[@class='contentItem somePadding']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//li[contains(.,'Bedroom')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[contains(.,'Bathrooms:')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//li[contains(.,'Carports: ')]/strong/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        deposit=response.xpath("//span[@class='ldBond']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].replace("$","").replace(",",""))
        item_loader.add_value("landlord_phone","02 4228 2555")
        property_type=response.url
        if "apartment" in property_type or "flat" in property_type:
            item_loader.add_value("property_type","apartment")
        if "house" in property_type:
            item_loader.add_value("property_type","house")
        


        yield item_loader.load_item()