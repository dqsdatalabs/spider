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
    name = 'townsville_harcourts_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Townsvilleharcourts_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.kingsberry.com.au/rent/for-rent/"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='frame']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            nextpage=f"https://www.kingsberry.com.au/rent/for-rent/page/{page}"
            if nextpage:
                yield Request(response.urljoin(nextpage), callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h2[@class='section_title']/h3/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h2[@class='section_title']/h3/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1].strip())
        rent=response.xpath("//div[@class='block price s-lrpad']/h3/strong/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("p")[0].strip().replace("\xa0","").replace(",",""))
            item_loader.add_value("currency","EUR")
        description=response.xpath("//div[@class='property-description s-lrpad']/text()").getall()
        if description:
            item_loader.add_value("description",description)
        square_meters=response.xpath("//strong[.='Building Size:']/parent::span/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        room_count=response.xpath("//span[.='bed']/preceding-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='bath']/preceding-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        images=[x for x in response.xpath("//li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        property_type=response.xpath("//strong[.='Property Type:']/parent::span/following-sibling::span/text()").get()
        if property_type and ("house" in property_type or "House"==property_type):
            item_loader.add_value("property_type","house")
        if property_type and ("Unit"==property_type or "Studio"==property_type):
            item_loader.add_value("property_type","studio")
        if property_type and "Offices"==property_type:
            return 
        email=response.xpath("//a[contains(@href,'mailto')]/@href").get()
        if email:
            item_loader.add_value("landlord_email",email)
        phone=response.xpath("//p[contains(.,'Telephone')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.strip().split(" ")[-1])
        name=response.xpath("//p[@class='name']/strong/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)

        yield item_loader.load_item()