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

class MySpider(Spider):
    name = 'aplusrealty_net_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        yield Request("https://www.aplusrealty.net.au/list_property.php?SearchType=2", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='products']/li"):
            follow_url = response.urljoin(item.xpath(".//h4/a/@href").get())
            property_type = " ".join(item.xpath(".//li[contains(.,'Type:')]/text()").getall()).split("(")[-1].split(")")[0]
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Aplusrealty_Net_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("au/")[1].split("/")[0])
        
        title = response.xpath("//div[@class='property_detail']//h3//text()").get()
        if title:
            item_loader.add_value("title", title)
            
        address = response.xpath("//title/text()").get()
        if address:
            item_loader.add_value("address", address.split("|")[0])
            item_loader.add_value("city", address.split("|")[0].split(",")[-3].strip())
            item_loader.add_value("zipcode", address.split("|")[0].split(",")[-4].strip())

        room_count = response.xpath("//div[contains(@class,'aminitis')]/text()[contains(.,'Bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        room_count = response.xpath("//div[contains(@class,'aminitis')]/text()[contains(.,'Bathroom')]").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count.strip().split(" ")[0]) 
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split("$")[1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
            
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'Parking')] | //div[contains(@class,'aminitis')]/text()[contains(.,'Car')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        pets_allowed = response.xpath("//li[contains(.,'Pet Friendly')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        utilities = response.xpath("//text()[contains(.,'Utilities') and contains(.,'$') ]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("$")[1].strip().split(" ")[0])
        
        description = " ".join(response.xpath("//div[@class='excerpt']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        if "m2" in description:
            item_loader.add_value("square_meters", description.split("m2")[0].strip().split(" ")[-1])
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_xpath("landlord_name", "//div[@class='our-info']/h5/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='our-info']/a[contains(@href,'tel')]/text()")
        item_loader.add_value("landlord_email", "info@aplusrealty.net.au")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None