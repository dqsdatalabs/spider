# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'loverealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Loverealestate_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.lovere.com.au/lease/residential-for-lease/"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='centerimage']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            nextpage=response.xpath("//a[@class='direction next']/@href").get()
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse)
                
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        property_type=response.xpath("//span[@class='property-type']/text()").get()
        if property_type and "House" in property_type:
            item_loader.add_value("property_type","house")
        if property_type and "Unit" in property_type:
            item_loader.add_value("property_type","studio")
        if property_type and "Apartment" in property_type:
            item_loader.add_value("property_type","apartment")
        adres=response.xpath("//h1[@class='address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//span[@class='suburb-state']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",int(rent.split("$")[-1].split("per")[0].split("PW")[0].split("Per")[0].split("pw")[0])*4)
        item_loader.add_value("currency","USD")
        images=[x for x in response.xpath("//img[@class='fit-width']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//span[.='Beds']/preceding-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//span[.='Bath']/preceding-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//span[.='Cars']/preceding-sibling::text()").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name","Love & Co")
 
        yield item_loader.load_item()