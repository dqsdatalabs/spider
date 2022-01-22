# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
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
    name = 'templetonproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Templetonproperty_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.templetonproperty.com.au/search/?mdf_sale_type=rental"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item-img']/a/@href").getall():
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
        adres=response.xpath("//header[@class='listing-header']/h2/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='header-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("per")[0])
        item_loader.add_value("currency","USD")
        room_count=response.xpath("//span[@class='bed']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[@class='bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//span[@class='car']/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//section[@class='listing-content']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        property_type=response.xpath("//div[@class='header-category']/text()").get()
        if property_type and "house" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "unit" in property_type.lower():
            item_loader.add_value("property_type","Studio")
        parking=response.xpath("//li[contains(.,'Garage Spaces:')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name","Templeton Property")


        yield item_loader.load_item()