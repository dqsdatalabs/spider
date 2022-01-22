# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'raymascaro_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://pentacle.tech/property/residential-rent/load-residential-rent-list.php"]
    handle_httpstatus_list = [500,403] 
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data["listing"]:
         
            status = item["AdTypeText"]
            if status and status.lower() != "lease":
                continue
            follow_url = f"https://www.realestateview.com.au/listings/{item['OrderID']}"
            yield Request(follow_url, callback=self.populate_item, meta={"p_type":item["PropertyType"], "ext_link":item["FullDisplayLink"],"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 
        
        item_loader.add_value("external_link", response.meta["ext_link"])
        f_text = response.meta["p_type"]
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Raymascaro_Com_PySpider_australia")   
        item = response.meta.get('item')
        item_loader.add_value("external_id", str(item["OrderID"])) 
        if "BedroomsCount" in item:
            item_loader.add_value("room_count", str(item["BedroomsCount"])) 
        if "BathroomsCount" in item:
            item_loader.add_value("bathroom_count", str(item["BathroomsCount"])) 
        item_loader.add_value("title", item["Title"]) 
        
        city = item["Suburb"]
        state = item["CityState"]
        zipcode = item["PostCode"]
        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        address = item["AddressText"]
        if city and zipcode:
            item_loader.add_value("address", address +", "+city+", "+state+" "+zipcode) 
        else:
            item_loader.add_value("address", address) 
        latitude = item["GeocodeLatitude"]
        longitude = item["GeocodeLongitude"]
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        parking = str(item["TotalParkingCount"])
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        item_loader.add_value("available_date", item["CreationDate"])
    
        rent =  item["Price"]
        if rent:
            if "per week" in rent:
                rent = rent.split("$")[-1].strip().split("p")[0].replace(",","")
                item_loader.add_value("rent", int(float(rent))*4)
            else:
                rent = rent.split("$")[-1].strip().split("p")[0].replace(",","")
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        if "PhotosCount" in item:
            images = [item[f"Photo{x}OriginalURL"] for x in range(1,int(item["PhotosCount"])+1)]
            if images:
                item_loader.add_value("images", images)
        
        item_loader.add_value("description", item["DescriptionNoHTML"])
        try:
            item_loader.add_value("landlord_name", item["ContactAgent1Name"])        
            item_loader.add_value("landlord_phone", item["ContactAgent1MobilePhone"])
            item_loader.add_value("landlord_email", item["ContactAgent1Email"])
        except: 
            pass
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None