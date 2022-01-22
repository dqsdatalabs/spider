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
    name = 'gumtree_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Gumtree_PySpider_ireland"

    def start_requests(self):
        url = "https://www.gumtree.ie/s-flat-house-for-rent/v1c8005p1"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):


        for url in response.xpath("//a[@class='href-link tile-title-text']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

        
        next_button = response.xpath("//span[@class='pag-box']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//span[.='Dwelling Type']/following-sibling::span/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        if not property_type:
            property_type1=response.xpath("//span[@class='myAdTitle']/text()").get()
            if get_p_type_string(property_type1):
                item_loader.add_value("property_type", get_p_type_string(property_type1))

        adres=",".join(response.xpath("//div[@class='location']//a//text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        
        rent=response.xpath("//div[@class='price']/span/span/text()").get()
        if rent and "€" in rent:
            item_loader.add_value("rent",rent.split("€")[1].replace(",",""))
        item_loader.add_value("currency","GBP")
        title=response.xpath("//span[@class='myAdTitle']/text()").get()
        if title:
            item_loader.add_value("title",title)
        description=response.xpath("//div[@class='description']/span/p/b/text() | //div[@class='description']/span/p/text()").getall()
        if description:
            item_loader.add_value("description",description)

        images=[x for x in response.xpath("//span[@class='vertical-alignment-helper']/following-sibling::img/@src | //div[@class='wrap']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//span[contains(.,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("bedroom")[0])
        furnished=response.xpath("//span[.='Furnished']/following-sibling::span/text()").get()
        if furnished and furnished=="Yes":
            item_loader.add_value("furnished",True)
        bathroom_count=response.xpath("//span[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("bathroom")[0])
        available_date=response.xpath("//span[.='Available']/following-sibling::span/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        pets_allowed=response.xpath("//span[.='Pet Friendly']/following-sibling::span/text()").get()
        if pets_allowed and "No"==pets_allowed:
            item_loader.add_value("pets_allowed",False)
        if pets_allowed and "Yes"==pets_allowed:
            item_loader.add_value("pets_allowed",True)
        parking=response.xpath("//span[.='Parking']/span/text()").get()
        if parking and "Garage" in parking:
            item_loader.add_value("parking",True)
        
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