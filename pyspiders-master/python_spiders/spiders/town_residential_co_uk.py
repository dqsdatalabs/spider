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
    name = 'town_residential_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["https://www.town-residential.co.uk/lettings"]

    custom_settings={"PROXY_ON": True}
    
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//li[contains(@class,'cell')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.town-residential.co.uk/lettings?p={page}&min_price=&max_price="
            yield Request(p_url, callback=self.parse, meta={'page': page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property/")[1].split("-")[0])
        item_loader.add_value("external_source", "Townresidential_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.xpath("//h1/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        item_loader.add_value("title", prop_type)
        
        address = response.xpath("//h1/text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.split("£")[1].split(" ")[0].replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        desc = "".join(response.xpath("//div[contains(@class,'property__text')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        
        if "bedroom" in prop_type:
            room_count = prop_type.split("bedroom")[0].strip()
            item_loader.add_value("room_count", room_count)
        elif "bedroom" in desc:
            item_loader.add_value("room_count", desc.split("bedroom")[0].strip().split(" ")[-1])
            
        images = [x for x in response.xpath("//div[@class='property__images']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "TOWN RESIDENTIAL")
        item_loader.add_value("landlord_phone", "020 3371 1790")
        item_loader.add_value("landlord_email", "info@town-residential.co.uk")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None