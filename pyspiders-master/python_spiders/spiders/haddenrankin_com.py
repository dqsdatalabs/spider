# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'haddenrankin_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source = "HaddenRankin_PySpider_united_kingdom"
    
    start_urls = ['https://www.haddenrankin.com/tenants/properties/?p=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//a[contains(.,'View')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.haddenrankin.com/tenants/properties/?p={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        title = response.xpath("//div[@class='h3']//text()").get()
        item_loader.add_value("title", title)
        
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        # else:
        #     return
        
        item_loader.add_value("external_source", self.external_source)

        available_date = response.xpath("//div[@class='date-available']//text()").get()
        if available_date:
            available_date = available_date.replace("Available","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        rent = response.xpath("//span[@class='property-price']//text()").get()
        if rent:
            rent = rent.split(" ")[0].replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,' parking')]")
        if parking:
            item_loader.add_value("parking", True)
        
        pets_allowed = response.xpath("//li[contains(.,'Pets')]")
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]")
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing = response.xpath("//li[contains(.,'Washing') or contains(.,' washing')]")
        if washing:
            item_loader.add_value("washing_machine", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]")
        if furnished:
            item_loader.add_value("furnished", True)
        
        description = "".join(response.xpath("//div[h2[contains(.,'Description')]]//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='lightbox-gallery']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Hadden Rankin")
        item_loader.add_value("landlord_phone", "0131 220 5241")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif "bungalow" in p_type_string.lower() or ("house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None