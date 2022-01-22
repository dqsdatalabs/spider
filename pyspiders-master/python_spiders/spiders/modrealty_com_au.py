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
    name = 'modrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Modrealty_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.modrealty.com.au/rent"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='listing-item']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)               
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div[@class='property-title']/h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//a[@class='listing-address']/i/following-sibling::text()").get()
        if adres:
            item_loader.add_value("address",adres)
        property_type=response.xpath("//div[@class='sub-price']/text()").get()
        if property_type and (property_type=="Flat" or property_type=="Apartment"):
            item_loader.add_value("property_type","apartment")
        if property_type and (property_type=="House" or "semi" in property_type.lower()):
            item_loader.add_value("property_type","house")
        if property_type and property_type=="Duplex":
            item_loader.add_value("property_type","house")
        images=[x for x in response.xpath("//div//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        rent=response.xpath("//div[@class='property-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("per")[0])
        item_loader.add_value("currency","USD")
        room_count=response.xpath("//li[contains(.,'Bedrooms')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[contains(.,'Bathrooms')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//li[contains(.,'Car Spaces')]/span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        description="".join(response.xpath("//h3[.='Description']/following-sibling::div//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        name=response.xpath("//div[@class='agent-details']/h4/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='agent-details']//span//a[contains(@href,'mailto')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//div[@class='agent-details']//span//a[contains(@href,'tel')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)

        yield item_loader.load_item()