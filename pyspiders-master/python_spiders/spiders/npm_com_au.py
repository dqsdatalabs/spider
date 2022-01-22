# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
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
    name = 'npm_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Npm_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.npm.com.au/search/?price_lower=200&price_higher=3000&keyword=&suburb=&bedrooms=1"},

        ] 
         # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='col-xs-12 post_image']/parent::a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page==2 or seen:
            nextpage=f"https://www.npm.com.au/search/?price_lower=200&price_higher=3000&keyword=&suburb=&bedrooms=1&page_number={page}"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={'page':page+1})
                

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='col-md-8 proj-title']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='col-md-8 proj-title']/h2/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("we")[0].split("$")[-1].strip())
        item_loader.add_value("currency","USD")
        description=response.xpath("//div[@class='col-md-8 col-md-pull-4 proj-feat-copy']//p/text()").get()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//li[@class='bed']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[@class='bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//li[@class='car']/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x.split("url('")[-1].split("')")[0] for x in response.xpath("//div[@class='item']/@style").getall()]
        if images:
            item_loader.add_value("images",images)
        name=response.xpath("//div[@class='agent-name no-image']/p/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='agent-name no-image']//p[2]/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        yield item_loader.load_item()