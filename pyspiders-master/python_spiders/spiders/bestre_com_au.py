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
    name = 'bestre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Bestre_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://bestre.com.au/rent/property-for-lease"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//li[@class='listingImage']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            nextpage=f"https://bestre.com.au/rent/property-for-lease/page-{page}"
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
        rent=response.xpath("//span[@class='listingPrice']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[-1].split("Per")[0])
        item_loader.add_value("currency","USD")
        description="".join(response.xpath("//div[@id='ldContentMain']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        adres=response.xpath("//div[@class='featureList']/following-sibling::strong/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        if not adres:
            adres=response.xpath("//span[@class='ldAddressDisplay']/text()").get()
            if adres:
                item_loader.add_value("address",adres)
        property_type=description 
        if "apartment" in property_type:
            item_loader.add_value("property_type","apartment")
        if "unit" in property_type:
            item_loader.add_value("property_type","apartment")
        if "house" in property_type or "home" in property_type:
            item_loader.add_value("property_type","house")
        room_count=response.xpath("//li[contains(.,'Bedroom:')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[contains(.,'Bathrooms')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//li[contains(.,'Garaging:')]/strong/text()").get()
        if parking and parking=="1":
            item_loader.add_value("parking",True)
        balcony=response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        deposit=response.xpath("//div[@class='ldBond']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].replace(",",""))
        images=[x for x in response.xpath("//span[@class='img-fl-sal']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Best Real Estate")
    


        yield item_loader.load_item()