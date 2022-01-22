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
    name = 'propertyinc_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Propertyinc_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://propertyinc.com.au/rent/"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//section[@id='for-rent-listings']/a/@href").getall():
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
        adres=response.xpath("//div[@class='address']/div/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='price-view']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("p")[0])
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//div[@class='featured-listing-bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[@class='featured-listing-bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//div[@class='featured-listing-garages']/text()").get()
        if parking and parking=="1":
            item_loader.add_value("parking",True)
        description="".join(response.xpath("//div[@class='property-description ']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        latitude=response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("latitude")[-1].split(";")[0].replace("=",""))
        longitude=response.xpath("//script[contains(.,'latitude')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("latitude")[-1].split(";")[0].replace("=",""))
        images=[x for x in response.xpath("//img[@class='property-photo']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        property_type=response.xpath("//div[@class='status']/text()").get()
        if property_type and "Unit" in property_type:
            item_loader.add_value("property_type","Studio")
        if property_type and "house" in property_type.lower():
            item_loader.add_value("property_type","House")

        item_loader.add_value("landlord_phone","07 3255 1230")
        item_loader.add_value("landlord_name","Property Inc")
    

        yield item_loader.load_item()