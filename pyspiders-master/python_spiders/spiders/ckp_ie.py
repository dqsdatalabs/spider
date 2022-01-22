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
    name = 'ckp_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Ckp_PySpider_ireland"

    def start_requests(self):
        url = "https://ckp.ie/properties-for-sale-and-to-rent-in-stillorgan-kilmacud-blackrock-donnybrook/residential-rentals/"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
       
        for item in response.xpath("//div[@class='jtg-box-image property-card--image']"):
            status=item.xpath(".//div[@class='property-card--status property-card--status-ribbon']/span[.='Let Agreed']/text()").get()
            if status:
                return 
            follow_url=item.xpath(".//div[@class='property-card--features']/following-sibling::a/@href").get()
            yield Request(response.urljoin(follow_url), callback=self.populate_item)
        next_button = response.xpath("//span[.='Next']/parent::a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse,)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//span[.='Type']/following-sibling::em/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        adres=response.xpath("//h1[@class='single-property-title single-property-title--default']//text()").get()
        if adres:
            item_loader.add_value("address",adres)
        title=response.xpath("//h1[@class='single-property-title single-property-title--default']//text()").get()
        if title:
            item_loader.add_value("title",title)
        
        rent=response.xpath("//span[.='Price']/following-sibling::em/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].split("Mont")[0].replace(",","").strip())
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//span[.='BEDROOMS']/following-sibling::em/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[.='BATHROOMS']/following-sibling::em/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        # external_id=response.xpath("//span[contains(.,'BER No:')]/strong/text()").get()
        # if external_id:
        #     item_loader.add_value("external_id",external_id)
        images=[x for x in response.xpath("//img[contains(@src,'mediaserver')]//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        desc=response.xpath("//div[@id='property-description']//p//text()").getall()
        if desc:
            item_loader.add_value("description",desc)
        # furnished=response.xpath("//ul//li[contains(.,'Furnished')]").get()
        # if furnished:
        #     item_loader.add_value("furnished",True)
        name=response.xpath("//h4[@class='strip-sidebar-agent-name has-agent-name']//text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//p[@class='strip-sidebar-agent-phone']//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//p[@class='strip-sidebar-agent-phone']//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
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