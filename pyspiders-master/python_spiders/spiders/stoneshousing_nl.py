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
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'stoneshousing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.stoneshousing.nl/aanbod_eindhoven/?price=0&price2=0&town=0&state=0"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property-bit']/.."):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.stoneshousing.nl/aanbod_eindhoven/index-{page}.html?price=0&price2=0&town=0&state=0"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//td[contains(.,'Type')]/following-sibling::td/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Stoneshousing_PySpider_netherlands")          
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        address = " ".join(response.xpath("//td[contains(.,'Adres')]/following-sibling::td//text()").getall())
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" in ")[1].strip())
        
        rent = response.xpath("//td[contains(.,'Prij')]/following-sibling::td//text()").get()
        if rent:
            price = rent.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//td[contains(.,'Kamers')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
            
        desc = " ".join(response.xpath("//h3[contains(.,'Besch')]//../div//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "m2" in desc:
            square_meters = desc.split("m2")[0].strip().split(" ")[-1]
            if square_meters.isdigit():
                item_loader.add_value("square_meters", square_meters)
        elif "m\u00b2" in desc:
            square_meters = desc.split("m\u00b2")[0].strip().split(" ")[-1]
            if square_meters.isdigit():
                item_loader.add_value("square_meters", square_meters)
                
        images = [response.urljoin(x) for x in response.xpath("//div[@class='property-photos']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "beschikbare" in desc:
            available_date = desc.split("beschikbare")[0].strip().split(" ")[-1]
            if "direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        floor = response.xpath("//td[contains(.,'Verdiepingen')]/following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//td[contains(.,'Huidige')]/following-sibling::td//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_name", "Stones Housing")
        item_loader.add_value("landlord_phone", "316-41555288")
        item_loader.add_value("landlord_email", "info@stoneshousing.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "woonboerderij" in p_type_string.lower()):
        return "house"
    else:
        return None