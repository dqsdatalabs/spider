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
    name = 'lovere_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
 
    def start_requests(self):
        start_url = "https://www.lovere.com.au/lease/residential-for-lease/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing')]//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//a[@class='direction next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Lovere_Com_PySpider_australia")
        property_type = "".join(response.xpath("//div[contains(@class,'description')]/text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[0].split("/")[-1])
        item_loader.add_xpath("title", "//title/text()")

        rent = response.xpath("//span[@class='price']/text()").re_first(r"\d+")
        if rent:         
            item_loader.add_value("rent",int(float(rent))*4)
        item_loader.add_value("currency","AUD")

        room_count = response.xpath("//li[@class='bed']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li[@class='bath']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        address = response.xpath("//h1[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-1]
            if city:
                item_loader.add_value("city", city.strip())
        zipcode = response.xpath("//span[@class='suburb-state']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        desc =  "".join(response.xpath("//div[contains(@class,'description')]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if 'parking' in desc.lower():
                item_loader.add_value("parking", True)
        
        features = "".join(response.xpath("//li[@class='active']/text()").getall())
        if features:
            if "parking" in features.lower():
                item_loader.add_value("parking", True)
        images = [ x for x in response.xpath("//div[@class='image']/a/img/@data-src|//div[@class='centerimage placeholder']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        name = response.xpath("//p[contains(@class,'name')]/a/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
            landlord_email = f'{name.split()[0].lower()}.{name.split()[0][0].lower()}@lovere.com.au'
            if landlord_email:
                item_loader.add_value("landlord_email", landlord_email)  
        else:
            item_loader.add_value("landlord_name", "Love & Co")
            item_loader.add_value("landlord_email", "reservoir@lovere.com.au")

        phone = response.xpath("//div[@class='details left']/div/p[@class='font-size-20']/text()").extract_first()
        if phone and any(p for p in phone if p.isdigit()):
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "(03) 9460 6511")

        lat_lng = response.xpath("//script[contains(.,'setView')]/text()").re_first(r"setView\(\[(-*\d+.\d+,\d+.\d+)\]")
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(",")[1])


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None