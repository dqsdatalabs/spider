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
    name = 'graylingproperties_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Graylingproperties_PySpider_ireland"

    def start_requests(self):
        url = "https://graylingproperties.ie/new-launches-dublin/"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='boxes even right']//a//@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.url
        if dontallow and ("faqs" in dontallow or "location" in dontallow):
            return 
        

        adres=response.xpath("//div[@class='box has-border']/div/h1/span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        description="".join(response.xpath("//h2[.='Features']/following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        property_type="".join(response.xpath("//h2[.='Features']/following-sibling::p//text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        room_count="".join(response.xpath("//h2[.='Features']/following-sibling::p//text()").getall())
        if room_count:
            room_count=room_count.split("Bedroom")[0].split("bed")[0].strip().split(" ")[-1]
            if room_count:
                item_loader.add_value("room_count",room_count)
        images=response.xpath("//div[@class='intro right']/@style").get()
        if images:
            item_loader.add_value("images",images.split("url('")[-1].split("');")[0])
        rent=response.xpath("//p[contains(.,'Rents')]/strong/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].split("per")[0].replace(",","").strip())
        item_loader.add_value("currency","GBP")
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None