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
    name = 'raywhite_realestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "CONCURRENT_REQUESTS": 3,
        # "AUTOTHROTTLE_ENABLED": True,
        # "AUTOTHROTTLE_START_DELAY": .1,
        # "AUTOTHROTTLE_MAX_DELAY": .3,
        "DOWNLOAD_DELAY": 3,
        "HTTPCACHE_ENABLED": False,
    }
    url = "https://lexa.realestate.com.au/graphql"
    headers = {
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': 'https://www.realestate.com.au',
        'referer': '',
    }

    def start_requests(self):
        infos = [
            {   'referer': 'https://www.realestate.com.au/',
                "payload" : {"operationName":"searchByQuery","variables":{"query":"{\"channel\":\"rent\",\"page\":1,\"pageSize\":25,\"filters\":{\"propertyTypes\":[\"unit apartment\"],\"surroundingSuburbs\":true,\"excludeNoSalePrice\":false,\"ex-under-contract\":false,\"furnished\":false,\"petsAllowed\":false,\"hasScheduledAuction\":false},\"localities\":[]}","testListings":False,"nullifyOptionals":False,"testId":"RentResults","recentHides":[]},"extensions":{"persistedQuery":{"version":1,"sha256Hash":"f8a7353b213f7dbce77ad95aa4cae9558511cd0617d3f10f5c26b534a480b570"}}},
                "property_type" : "apartment",
            },
            {   'referer': 'https://www.realestate.com.au/',
                "payload" : {"operationName":"searchByQuery","variables":{"query":"{\"channel\":\"rent\",\"page\":1,\"pageSize\":25,\"filters\":{\"propertyTypes\":[\"house\",\"townhouse\",\"villa\"],\"surroundingSuburbs\":true,\"excludeNoSalePrice\":false,\"ex-under-contract\":false,\"furnished\":false,\"petsAllowed\":false,\"hasScheduledAuction\":false},\"localities\":[]}","testListings":False,"nullifyOptionals":False,"testId":"RentResults","recentHides":[]},"extensions":{"persistedQuery":{"version":1,"sha256Hash":"f8a7353b213f7dbce77ad95aa4cae9558511cd0617d3f10f5c26b534a480b570"}}},
                "property_type" : "house"
            },
        ]
        for item in infos:
            self.headers["referer"] = item["referer"]
            print(type(item["payload"]))
            yield Request(self.url,
                        method="POST",
                        headers=self.headers,
                        body=json.dumps(item["payload"]),
                        dont_filter=True,
                        callback=self.parse,                   
                        meta={'property_type': item["property_type"], 'payload': item["payload"], 'referer': item["referer"]})
            break
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]["rentSearch"]["results"]["exact"]["items"]:
            seen = True
            item_loader = ListingLoader(response=response)

            data = item["listing"]

            item_loader.add_value("external_source", "Raywhite_Realestate_Com_PySpider_australia")
            item_loader.add_value("external_link", data["_links"]["canonical"]["href"])
                    
            prop_type = data["propertyType"]["id"]
            if prop_type and "studio" in prop_type.lower():
                item_loader.add_value("property_type", "studio")
            else:   
                item_loader.add_value("property_type", response.meta["property_type"])

            external_id = data["id"]
            item_loader.add_value("external_id", str(external_id))
            
            address = data["address"]["display"]["fullAddress"]
            if address:
                item_loader.add_value("address", address)
            else:
                address = data["address"]["suburb"]
                item_loader.add_value("address", address)
                
            item_loader.add_value("title", address)

            lat_lng = "".join(response.xpath("substring-after(//script//text()[contains(.,'latitude')],'latitude')").extract())
            if lat_lng:
                item_loader.add_value("latitude", lat_lng.split(",")[0].replace('":','').strip())
                lon = response.xpath("substring-before(substring-after(//script//text()[contains(.,'longitude')],'longitude'),',')").extract_first()
                item_loader.add_value("longitude", lon.replace('":','').strip())
            
            try:
                latitude = data["address"]["display"]["geocode"]["latitude"]
                if latitude:
                    item_loader.add_value("latitude", str(latitude))
            except: pass
            try:
                longitude = data["address"]["display"]["geocode"]["longitude"]
                if longitude:
                    item_loader.add_value("longitude", str(longitude))
            except: pass
            
            city = data["address"]["suburb"]
            item_loader.add_value("city", city)
            
            zipcode = data["address"]["postcode"]
            item_loader.add_value("zipcode", zipcode)
            
            rent = data["price"]["display"]
            if rent:
                try:
                    if "$" in rent:
                        price = rent.lower().replace(",","").split("$")[1].replace("per","").strip().replace("pw"," ").replace("p/w","").replace("-"," ").replace("/"," ").split(" ")[0]
                        item_loader.add_value("rent", int(float(price))*4)
                        rent = int(float(price))*4
                    elif rent.lower().strip().replace("/"," ").replace("pw"," ").replace(".","").split(" ")[0].isdigit():
                        price = rent.lower().strip().replace("/"," ").replace("pw"," ").split(" ")[0]
                        item_loader.add_value("rent", int(float(price))*4)
                        rent = int(float(price))*4
                    elif "week" in rent.lower():
                        price = rent.split(" ")[0].replace(",","")
                        item_loader.add_value("rent", int(float(price))*4)
                        rent = int(float(price))*4
                    if not rent.isdigit() and ("deposit" in rent.lower() or "leased" in rent.lower() or "holding" in rent.lower()):
                        return
                except: pass
                
            item_loader.add_value("currency", "USD")
            
            room_count = data["generalFeatures"]["bedrooms"]["value"]
            item_loader.add_value("room_count", room_count)
            
            bathroom_count = data["generalFeatures"]["bathrooms"]["value"]
            item_loader.add_value("bathroom_count", bathroom_count)
            
            parking = data["generalFeatures"]["parkingSpaces"]["value"]
            if parking:
                item_loader.add_value("parking", True)
            
            images = data["media"]["images"]
            for image in images:
                item_loader.add_value("images", image["templatedUrl"])

            floorplan = data["media"]["floorplans"]
            for floorp in floorplan:
                item_loader.add_value("floor_plan_images", floorp["templatedUrl"])
            
            try:
                deposit = data["bond"]["display"]
                if deposit:
                    deposit = deposit.replace(",","").replace("$","")
                    item_loader.add_value("deposit", int(float(deposit)))
            except: pass
            
            from datetime import datetime
            import dateparser
            available_date = data["availableDate"]["display"]
            if available_date:
                available_date = available_date.split("Available")[1].strip()
                if "now" not in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
            
            desc = data["description"]
            item_loader.add_value("description", desc)
            
            # features = data["propertyFeatures"]
            # for i in features:
            #     if i["displayLabel"] == "Furnished":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("furnished", True)
            #     if i["displayLabel"] == "Dishwasher":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("dishwasher", True)
            #     # if i["displayLabel"] == "Land size":
            #     #     sq_m = i["value"]["displayValue"]
            #     #     if sq_m:
            #     #         print(sq_m)
            #             # item_loader.add_value("furnished", True)
            #     if i["displayLabel"] == "Secure parking":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("parking", True)
            #     if i["displayLabel"] == "Garage spaces":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("parking", True)
            #     if i["displayLabel"] == "Carport spaces":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("parking", True)
                
            #     if i["displayLabel"] == "Pets allowed":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("pets_allowed", True)
                
            #     if i["displayLabel"] == "Balcony":
            #         furnished = i["value"]
            #         if furnished:
            #             item_loader.add_value("balcony", True)
                    
            item_loader.add_value("landlord_name", "RAYWHITE REAL ESTATE")
            item_loader.add_value("landlord_phone", data["listingCompany"]["businessPhone"])
            
            yield item_loader.load_item()
        
        if page == 2 or seen:
            payload = response.meta["payload"]
            payload["variables"]["query"].replace('page":1', 'page":' + str(page))
            self.headers["referer"] = response.meta["referer"]
            yield Request(self.url,
                        method="POST",
                        headers=self.headers,
                        body=json.dumps(payload),
                        dont_filter=True,
                        callback=self.parse,                   
                        meta={'property_type': response.meta["property_type"], 'payload': response.meta["payload"], 'referer': response.meta["referer"], 'page': page+1})