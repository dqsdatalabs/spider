# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'classifieds-advertiser_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "ClassifiedsAdvertiser_PySpider_ireland"

    def start_requests(self):
        url = "http://classifieds.advertiser.ie/property/to-let/"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//div[@class='resultItemMainHolder']/h4/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        if page == 2 or seen:            
            p_url = f"http://classifieds.advertiser.ie/property/to-let/?p={page}"
            yield Request(p_url,callback=self.parse,dont_filter=True,meta={"page":page+1,})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        images=[x for x in response.xpath("//ul[@class='imageList']//li//a//@href").getall()]
        if images:
            item_loader.add_value("images",images)
        property_type=response.xpath("//th[.='Property Type']/following-sibling::td/text()").get()
        if property_type:
            item_loader.add_value("property_type",property_type)
        adres=response.xpath("//th[.='Region']/following-sibling::td/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        room_count=response.xpath("//th[.='Bedrooms']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        description=response.xpath("//div[@style='width: 100%; text-align:justify;']/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//span[.='Price:']/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].replace(",",""))
        phone=response.xpath("//span[.='Phone:']/following-sibling::div/span/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        yield item_loader.load_item()