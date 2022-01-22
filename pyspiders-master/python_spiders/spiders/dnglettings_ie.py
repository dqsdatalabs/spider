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
    name = 'dnglettings_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Dnglettings_PySpider_ireland"

    def start_requests(self):
        url = "https://gillespielowe.ie/properties/"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[@class='elive_property_listings_ad_images']/a/@href").getall():
            yield Request(url, callback=self.populate_item)
        next_button = response.xpath("//a[@class='page-numbers page-link']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//span[@class='elive_property_addetail_price']//following-sibling::span/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        adres=response.xpath("//div[@class='elive_property_addetail_header']//h2/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        title=response.xpath("//div[@class='elive_property_addetail_header']//h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        
        rent=response.xpath("//span[@class='elive_property_addetail_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("Mont")[0].replace(",","").strip())
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//span[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        external_id=response.xpath("//span[contains(.,'BER No:')]/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        images=[x for x in response.xpath("//div[@class='elive_property_addetail_thumbnails_list_imgcont']//a//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        desc=response.xpath("//div[@class='elive_property_addetail_propdescr_text']/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        furnished=response.xpath("//ul//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished",True)
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None