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
    name = 'elliottandsmith_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    download_timeout = 300 
    custom_settings = {
        "PROXY_ON" : True
    }
    def start_requests(self):

        url = "https://api.dezrez.com/api/simplepropertyrole/search?apikey=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguZGV6cmV6LmNvbS9BcGlLZXlJc3N1ZXIiLCJhdWQiOiJodHRwczovL2FwaS5kZXpyZXouY29tL3NpbXBsZXdlYmdhdGV3YXkiLCJuYmYiOjE1MDcxOTIwMDEsImV4cCI6NDY2Mjg2NTYwMSwiSXNzdWVkVG9Hcm91cElkIjoiNDgxODk3NyIsIkFnZW5jeUlkIjoiMjk1Iiwic2NvcGUiOlsiaW1wZXJzb25hdGVfd2ViX3VzZXIiLCJwcm9wZXJ0eV9iYXNpY19yZWFkIiwibGVhZF9zZW5kZXIiXX0.07DuWhtt3fOwkc7Z9jWADXdAVUu0xlL24V-YpA1bTIU"
        payload="{\r\n    \"BranchIdList\": [],\r\n    \"MinimumPrice\": \"0\",\r\n    \"MaximumPrice\": \"99999997\",\r\n    \"MinimumBedrooms\": \"0\",\r\n    \"PageSize\": 1000,\r\n    \"RoleTypes\": [\r\n        \"Letting\"\r\n    ],\r\n    \"MarketingFlags\": [\r\n        \"ApprovedForMarketingWebsite\"\r\n    ],\r\n    \"PropertyTypes\": [],\r\n    \"SortBy\": 0,\r\n    \"SortOrder\": \"1\"\r\n}"
        headers = {
            'Rezi-Api-Version': ' 1.0',
            'Content-Type': 'application/json'
        }
        yield Request(
            url=url,
            callback=self.parse,
            body=payload,
            headers=headers,
            method="POST",
        )


    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["Collection"]:            
            if "PropertyType" in item and item["PropertyType"]:
                prop_type = item["PropertyType"]["DisplayName"]
                if prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
                    prop_type = "apartment"
                elif prop_type and ("house" in prop_type.lower() or "maisonette" in prop_type.lower()):
                    prop_type = "house"
                elif prop_type and "studio" in prop_type.lower():
                    prop_type = "studio"
                elif prop_type and "room" in prop_type.lower():
                    prop_type = "room"
                else:
                    prop_type = None
            else:
                prop_type = None
            
            if "SummaryTextDescription" and item["SummaryTextDescription"]:
                desc = item["SummaryTextDescription"]
            else:
                desc = None
            p_id = item["RoleId"]
            follow_url = f"http://www.elliottandsmith.co.uk/Property.aspx?pid={p_id}"
            yield Request(follow_url, callback=self.populate_item, meta={"prop_type":prop_type, "desc":desc,"p_id":p_id,"item":item})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", str(response.meta.get("p_id")))

        item_loader.add_value("external_source", "Elliottandsmith_PySpider_"+ self.country + "_" + self.locale)
        property_type = response.meta.get("prop_type")
        if property_type:
            item_loader.add_value("property_type", property_type)
        else:
            desc = remove_tags(response.meta.get("desc"))
            if desc:
                if desc and ("apartment" in desc.lower() or "flat" in desc.lower()):
                   item_loader.add_value("property_type", "apartment")
                elif desc and ("house" in desc.lower() or "maisonette" in desc.lower()):
                    item_loader.add_value("property_type", "house")
                elif desc and "studio" in desc.lower():
                    item_loader.add_value("property_type", "studio")
                elif desc and "room" in desc.lower():
                    item_loader.add_value("property_type", "room")
                else:
                    return
            else:
                return
        item = response.meta.get("item")
        # print("-------------",item)
        desc = response.meta.get("desc")
        street = item["Address"]["Street"]
        town = item["Address"]["Town"]
        county = item["Address"]["County"]
        address = street+", "+town+", "+county        
        zipcode = item["Address"]["Postcode"]
        lat = item["Address"]["Location"]["Latitude"]
        lng = item["Address"]["Location"]["Longitude"]
        images = item["Images"]         
        currency = item["Price"]["CurrencyCode"]
        price = item["Price"]["PriceValue"]
        price_type = item["Price"]["PriceType"]["DisplayName"]
        bathrooms = item["RoomCountsDescription"]["Bathrooms"]
        bedrooms = item["RoomCountsDescription"]["Bedrooms"]
        if address:
            item_loader.add_value("title",address.strip())
            item_loader.add_value("address",address.strip())
        if county:
            item_loader.add_value("city",county)

        item_loader.add_value("zipcode",zipcode)
        item_loader.add_value("latitude",str(lat))
        item_loader.add_value("longitude",str(lng))
        item_loader.add_value("currency",currency)
        if "Monthly" not in price_type:
            price = int(float(price))*4
        item_loader.add_value("rent",int(float(price)))
        item_loader.add_value("bathroom_count",bathrooms)
        item_loader.add_value("room_count",bedrooms)
        if desc:
            item_loader.add_value("description",desc)
            if "no pets" in desc.lower():
                item_loader.add_value("pets_allowed",False)
            if "parking" in desc.lower():
                item_loader.add_value("parking",True)
            if "unfurnished" in desc.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished",True)   
            if "available" in desc.lower():
                try:           
                    available_date = desc.lower().split("available")[1] 
                    if "IMMEDIATELY" in available_date.upper():
                        available_date = "now"
                    else:
                        available_date = available_date.split("(")[0]
                    date_parsed = dateparser.parse(available_date, languages=['en'])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
                except:
                    pass
                    
        images = []
        for x in item["Images"]:      
            images.append(x["Url"])
        if images:
            item_loader.add_value("images", images)

        floor_image = []
        floor_image = item["Documents"]
        for i in floor_image:
            floor_plan = i["DocumentSubType"]["DisplayName"]
            if floor_plan == "Floorplan":
                url = i["Url"]
                item_loader.add_value("floor_plan_images", url)

        item_loader.add_value("landlord_phone", "01268 947 947")
        item_loader.add_value("landlord_email", "lettings@elliottandsmith.co.uk")
        item_loader.add_value("landlord_name", "Elliott & Smith")
        yield item_loader.load_item()
