# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *
import json

class ImmowhitehouseSpider(scrapy.Spider):
    name = "immowhitehouse"
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    start_urls = ["https://www.whitehouse.immo/page-data/nl/te-huur/page-data.json"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
            p_type = item["MainTypeName"]
            if get_p_type_string(p_type): prop_type = get_p_type_string(p_type)
            else:
                continue

            ext_id = str(item["ID"])
            title = item["TypeDescription"]
            street = item["Street"]
            house_number = item["HouseNumber"]
            city = item["City"]
            zipcode = item["Zip"]
            lat = item["GoogleX"]
            lng = item["GoogleY"]
            square = item["SurfaceTotal"]
            bed = item["NumberOfBedRooms"]
            bath = item["NumberOfBathRooms"]
            desc = item["DescriptionA"]
            rent = item["Price"]
   
            # "HasLift":false
            elevator = item["HasLift"] 
            terrace = item["HasTerrace"] 
            # "NumberOfGarages":0
            parking = item["NumberOfGarages"]
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            item_loader.add_value("external_id", ext_id)
            
            images = [x for x in item["LargePictures"]]
            if images:
                item_loader.add_value("images", images)
            
            item_loader.add_value("address",  "{} {}, {} {}".format(street,house_number,zipcode,city))
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("property_type", prop_type)
            item_loader.add_value("title", title)
            item_loader.add_value("description", desc)
            item_loader.add_value("square_meters", square)
            
            item_loader.add_value("room_count", str(bed))
            item_loader.add_value("bathroom_count", str(bath))
            item_loader.add_value("rent", str(rent))
            item_loader.add_value("currency", "EUR")
            if elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
            if terrace:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)    

            if parking and parking != 0:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)  
            item_loader.add_value("landlord_email", "info@whitehouse.immo")
            item_loader.add_value("landlord_phone", "+32 (0)57 69 99 70")
            item_loader.add_value("landlord_name", "White House")

            ext_link = title.lower().replace(".","").replace("!","").replace("?","").replace('"','').replace('\\','')
            ext_link = ext_link[0:68].replace("-","").replace(" ","-")
            city = city.lower()
            external_link = f"https://www.whitehouse.immo/nl/te-huur/{city}/{ext_link}/{ext_id}/"
            item_loader.add_value("external_link", external_link)

            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bebouwing" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None

   