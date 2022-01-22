# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
class MySpider(Spider):
    name = 'hoganestates_com' 
    execution_type='testing'
    country='ireland'
    locale='en'
    external_source="Hoganestates_PySpider_ireland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hoganestates.com/residential-nav-bar/residential-for-rent",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@role='list']//div[@role='listitem']//a[@class='preview-link-2 w-inline-block']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//p[@class='paragraph-15']/text()").get()
        item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))

        dontallow=response.xpath("//div[@class='tag-sell']/text()").get()
        if dontallow and "Sale" in dontallow:
            return
        adres=response.xpath("//div[@class='title-column']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        property_type=response.xpath("//p[@class='paragraph-15']/text()").get()
        if property_type:
            if "House" in property_type:
                item_loader.add_value("property_type","house")
            if "apartment" in property_type.lower():
                item_loader.add_value("property_type","apartment")
        rent=response.xpath("//div[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].strip().replace(",",""))
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//div[.='Bedrooms']/preceding-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        description=response.xpath("//div[@class='w-richtext']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@role='list']//div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Rebecca Daly")
        item_loader.add_value("landlord_phone","014627101")
        yield item_loader.load_item()