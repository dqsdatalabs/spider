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
    name = 'christophershaw_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Host": "api.innovagent.property",
        "Origin": "https://christophershaw.co.uk",
        "Referer": "https://christophershaw.co.uk/",
    }   
    
    def start_requests(self):
        url = "https://api.innovagent.property/propertysearch/v1/letting"
        
        yield Request(
            url,
            callback=self.parse,
            headers=self.headers,
        )
    

    # 1. FOLLOWING
    def parse(self, response):

        for item in json.loads(response.body)["data"]["Data"]:
            item_loader = ListingLoader(response=response)
            oid = item["OID"]

            if item["IsTenancyAdvertised"] == 0: continue

            prop_type = item["PropertyType"]
            if prop_type and "share" in prop_type.lower():
                item_loader.add_value("property_type", "room")
            elif prop_type and ("flat" in prop_type.lower() or "apartment" in prop_type.lower()):
                item_loader.add_value("property_type", "apartment")
            elif prop_type and "house" in prop_type.lower():
                item_loader.add_value("property_type", "house")
            elif prop_type and "studio" in prop_type.lower():
                item_loader.add_value("property_type", "studio")
            else:
                continue
            item_loader.add_value("external_link", f"https://christophershaw.co.uk/property-to-rent#details/{oid}")
            item_loader.add_value("external_id", oid)
            item_loader.add_value("address", item["FullAddress"])
            item_loader.add_value("title", item["FullAddress"])
            item_loader.add_value("zipcode", item["PostCode"])
            item_loader.add_value("description", item["Description"])
            item_loader.add_value("available_date", item["AvailabilityDate"])
            item_loader.add_value("rent", item["PurchasePrice"])
            item_loader.add_value("currency", "GBP")            
            city = item["Address3"]
            if city:
                item_loader.add_value("city",city)
            deposit = item["Deposit"]
            if deposit !=0:
                item_loader.add_value("deposit", int(float(deposit)))
            room = item["Bedrooms"]
            if room !=0:
                item_loader.add_value("room_count", str(room))
            else:
                item_loader.add_value("room_count", "1")
            bathroom = item["Bathrooms"]
            if bathroom !=0:
                item_loader.add_value("bathroom_count", bathroom)          

            furnished = item["PropertyStatus"]
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

            yield Request(
                f"https://api.innovagent.property/propertysearch/v1/letting/image/{oid}",
                callback=self.populate_image,
                meta={"item_loader":item_loader, "oid":oid},
                headers=self.headers,
            )
    
    def populate_image(self, response):
        main_oid = response.meta["oid"]
        item_loader = response.meta.get("item_loader")
        item_loader.add_value("external_source", "Christophershaw_PySpider_"+ self.country + "_" + self.locale)
        image_list = []
        for image in json.loads(response.body)["data"]["Data"]["Images"]["Data"]:
            oid = image["OID"]
            image_list.append(f"https://propertysearch.innovagent.property/img/122/gallery/{oid}.jpg")
        
        item_loader.add_value("images", image_list)
        floorimage_list = []
        for floorimage in json.loads(response.body)["data"]["Data"]["Floorplans"]["Data"]:
            oid = floorimage["OID"]
            floorimage_list.append(f"https://propertysearch.innovagent.property/img/122/floorplans/{oid}.jpg")
        
        item_loader.add_value("floor_plan_images", floorimage_list)
        item_loader.add_value("landlord_phone", "01202 554 470")
        item_loader.add_value("landlord_name", "Christopher Shaw")

        yield Request(
                f"https://api.innovagent.property/propertysearch/v1/letting/feature/{main_oid}",
                callback=self.populate_feature,
                meta={"item_loader":item_loader},
                headers=self.headers,
            )

    def populate_feature(self, response):
        item_loader = response.meta.get("item_loader")
        
        data = json.loads(response.body)["data"]["Data"]
        for item in data:
            if "parking" in item["Name"].lower():
                item_loader.add_value("parking",True)
                break

        yield item_loader.load_item()
        