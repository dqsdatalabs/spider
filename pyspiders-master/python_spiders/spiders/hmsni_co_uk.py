# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'hmsni_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_url = "https://hmsni.co.uk/!/Fetch/collection/listings"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["data"]:
            property_type = item["property_type"] if "property_type" in item.keys() else None
            status = item["state"] if "state" in item.keys() else None
            if status == "For Rent" and property_type:
                if get_p_type_string(property_type):
                    yield Request(item["permalink"], callback=self.populate_item, meta={"property_type": get_p_type_string(property_type),"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Hmsni_Co_PySpider_united_kingdom")
        item = response.meta.get("item")

        rented = response.xpath("//p[contains(.,'Currently Let')]").get()
        if rented: return

        address = item["title"]
        if address:
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("title", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        images = item["images"]
        for image in images:
            item_loader.add_value("images", image)

        try:
            rent = item["price"]
            if rent:
                item_loader.add_value("rent", rent)
        except: pass
        item_loader.add_value("currency", "GBP")

        room_count = item["bedrooms"]
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = item["bathrooms"]
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = item["house_details"]
        if desc:
            item_loader.add_value("description", desc)
        
        furnished = item["furnished"]
        if furnished:
            item_loader.add_value("furnished",True)
        
        key_features = item["key_features"]
        for i in key_features:
            if "Available" in i:
                from datetime import datetime
                import dateparser
                available_date = i.split("Available")[1].strip()
                if available_date:
                    if "now" in available_date.lower():
                        item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                    else:
                        date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)
            if "balcony" in i.lower():
                item_loader.add_value("balcony", True)
            if "parking" in i.lower():
                item_loader.add_value("parking", True)
            if "floor" in i:
                floor = i.split("floor")[0].strip()
                item_loader.add_value("floor", floor)
                
        item_loader.add_value("landlord_name", "HOME MANAGEMENT SERVICES")
        item_loader.add_value("landlord_phone", "02890423341")
        item_loader.add_value("landlord_email", "info@hmsni.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "terraced" in p_type_string.lower()):
        return "house"
    else:
        return None