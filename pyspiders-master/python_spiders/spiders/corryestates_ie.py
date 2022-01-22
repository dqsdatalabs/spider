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
    name = 'corryestates_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Corryestates_PySpider_ireland"

    def start_requests(self):
        url = "http://www.corryestates.ie/rentals/results?status=8|11"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
       
        for item in response.xpath("//a[contains(@class,'propertyLink')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        next_button = response.xpath("//span[@id='listings_next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        adres=response.xpath("//section[@class='col-md-8']/h1/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        item_loader.add_value("zipcode",adres.split(",")[-1].strip())
        title=response.xpath("//section[@class='col-md-8']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//h2[@class='descriptiveTitle']/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        rent=response.xpath("//section[@class='col-md-8']/h1/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("Mont")[0].replace(",","").replace("/","").strip())
        images=[x for x in response.xpath("//img[@class='img-responsive pull-left']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//h3[.='Description']/following-sibling::p//text()").getall()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//h2[@class='descriptiveTitle']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("Bed")[0])
        phone=response.xpath("//li[@id='AgentPhone']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.replace("Call","").strip())
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