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
    name = 'mgmletting_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_url = "https://www.mgmletting.co.uk/properties.asp?Page=1&O=Status&Dir=DESC&branch=&propind=L&Country=&Location=&Town=&Area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&sleeps=&propType=&Furn=&FA=&LetType=&Cat=&Avail=&searchbymap=&locations=&SS=&fromdate=&todate=&minbudget=&maxbudget=&ref="

    def start_requests(self): # LEVEL 1
        yield Request(url=self.start_url,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'searchprop')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//div[contains(@class,'next')]//@href").get()
        if next_page:       
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[contains(@class,'proptype')]//text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_source", "Mgmletting_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("?Id=")[1].split("&")[0])

        title = response.xpath("//div[contains(@class,'address')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'address')]//text()").get()
        if address:
            address = address.split("-")[-1].strip()
            if ","in address:
                zipcode = address.split(",")[-1].strip()
                city = address.split(",")[-2].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = address.split(" ")[-1].strip()
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)

        rent = "".join(response.xpath("//div[contains(@class,'price')]/text()").getall())
        if rent:
            rent = rent.strip().replace("£","").split(".")[0]
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'bed')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'thumbphotocontainer')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[contains(@class,'available')]//text()").get()
        if available_date:
            available_date = available_date.split(":")[-1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        energy_label = response.xpath("//div[contains(@class,'description')]//text()[contains(.,'EPC')]").get()
        if energy_label:
            if "=" in energy_label:
                energy_label = energy_label.split("=")[1].strip()
            elif "-" in energy_label:
                energy_label = energy_label.split("-")[1].strip()
            else:
                energy_label = energy_label.strip().split(" ")[-1].strip()
            item_loader.add_value("energy_label", energy_label)
            
        dishwasher = response.xpath("//div[contains(@class,'features')]//li[contains(.,'DISHWASHER') or contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "MGM Letting")
        item_loader.add_value("landlord_phone", "0151 733 3407")
        item_loader.add_value("landlord_email", "info@mgmletting.co.uk")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terraced" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None