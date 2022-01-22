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
    name = 'alkilium_com'
    execution_type = 'testing'
    country = 'spain'
    locale ='es'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"1", "house":"2"}

        for key, value in kwargs.items():
            formdata = {
                "operation": "a",
                "min_price": "",
                "max_price": "",""
                "type": value,
                "location": "",
                "zone": "",
                "min_meters": "",
                "max_meters": "",
                "rooms": "", 
                "bathrooms": "",
                "elevator": "",
                "garage": "",
                "storage": "",
                "terrace": "",
                "search_type": "",
                "search_order": "",
                "search_code": "",
                "search_ref": "",
                "limit": "0,24",
                "search_group": "false"
            }
            yield FormRequest("https://alkilium.com/es/action/get_data_property_search",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': key,"type_value":value})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 24)
        type_value = response.meta.get("type_value")
        seen = False
        data = json.loads(response.body)
        if data["property_search"]: 
            for item in data["property_search"]:
                f_id = item["propertyCode"]
                url = f"https://alkilium.com/es/propiedad/{f_id}"
                seen = True
                item_loader = ListingLoader(response=response)

                item_loader.add_value("property_type", response.meta.get('property_type'))
                item_loader.add_value("external_link", url)
                item_loader.add_value("external_id", str(f_id))
                item_loader.add_value("external_source", "LangestionesInmo_PySpider_spain")
                latitude = item["propertyAddress"]["addressCoordinatesLatitude"]
                longitude = item["propertyAddress"]["addressCoordinatesLongitude"]
                zipcode = item["propertyAddress"]["addressZoneCode"]
                city = item["propertyAddress"]["addressZone"]
                addressTown = item["propertyAddress"]["addressTown"]
                addressProvince = item["propertyAddress"]["addressProvince"]
                address = f"{city}, {addressTown}, {addressProvince}"
                room_count = item["propertyFeatures"]["featuresRooms"]
                bathroom_count = item["propertyFeatures"]["featuresBathroomNumber"]
                square_meters = item["propertyFeatures"]["featuresAreaConstructed"]
                parking = item["propertyFeatures"]["featuresGarage"]
                swimming_pool = item["propertyFeatures"]["featuresPool"]
                terrace = item["propertyFeatures"]["featuresTerrace"]
                desc = item["propertyDescription"]
                rent = item["propertyOperation"]["operationPriceRent"]

                if square_meters:
                    item_loader.add_value("square_meters", int(float(square_meters)))
                    
                item_loader.add_value("longitude", str(longitude))
                item_loader.add_value("latitude", str(latitude))
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("rent", rent)
                item_loader.add_value("title", address)
                item_loader.add_value("address", address)
                item_loader.add_value("room_count", room_count)
                item_loader.add_value("bathroom_count", bathroom_count)
                item_loader.add_value("currency", "EUR")
                item_loader.add_value("landlord_name", "Alkilium")
                item_loader.add_value("landlord_phone", "(+34) 944 419 300")
                item_loader.add_value("landlord_email", "gestiondedatos@inmb.es")
                if terrace:
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)
                
                if swimming_pool:
                    item_loader.add_value("swimming_pool", True)
                else:
                    item_loader.add_value("swimming_pool", False)

                if parking:
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)

                if desc:
                    item_loader.add_value("description", desc.strip())

                images = [x for x in item["propertyImages"]]
                if images:
                    item_loader.add_value("images", images) 
                yield item_loader.load_item()
                
            if seen:            
                formdata = {
                    "operation": "a",
                    "min_price": "",
                    "max_price": "",""
                    "type": type_value,
                    "location": "",
                    "zone": "",
                    "min_meters": "",
                    "max_meters": "",
                    "rooms": "", 
                    "bathrooms": "",
                    "elevator": "",
                    "garage": "",
                    "storage": "",
                    "terrace": "",
                    "search_type": "",
                    "search_order": "",
                    "search_code": "",
                    "search_ref": "",
                    "limit": f"{page},24",
                    "search_group": "false"
                }
                yield FormRequest("https://alkilium.com/es/action/get_data_property_search",
                                callback=self.parse,
                                formdata=formdata,
                                dont_filter=True,
                                meta={"page":page+24, "property_type":response.meta["property_type"], "type_value":response.meta.get("type_value")})
                    
    
 