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
    name = "expertimmoorta"
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","
    external_source = "Experimmoorta_PySpider_belgium"
    start_urls = ["https://www.experimmoorta.be/page-data/nl/te-huur/page-data.json"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
            p_type = item["MainTypeName"]
            language = item["language"]
            if language != "nl":
                continue

            if item["SubStatusName"] == "Verhuurd":
                continue
            if get_p_type_string(p_type): 
                prop_type = get_p_type_string(p_type)
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
   
            #

            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_source", self.external_source)
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
            item_loader.add_value("square_meters", int(float(square)))
            
            item_loader.add_value("room_count", str(bed))
            item_loader.add_value("bathroom_count", str(bath))
            item_loader.add_value("rent", str(rent))
            item_loader.add_value("currency", "EUR")
          
            item_loader.add_value("landlord_email", "orta@experimmo.be")
            item_loader.add_value("landlord_phone", "055 20 74 20")
            item_loader.add_value("landlord_name", "EXPER IMMO ORTA")
            #  "HasLift":false
            if "HasLift" in item:
                elevator = item["HasLift"]
                if elevator:
                    item_loader.add_value("elevator", True)
                else:
                    item_loader.add_value("elevator", False)
            if "HasTerrace" in item:
                terrace = item["HasTerrace"] 
                  
                if terrace:
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)    
            # "NumberOfGarages":0
            if "NumberOfGarages" in item:
                parking = item["NumberOfGarages"]
                if parking and parking != 0:
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)  

            ext_link = title.lower().replace(".","").replace("!","").replace("?","").replace('"','').replace('\\','')
            ext_link = ext_link[0:68].replace("-","").replace(" ","-")
            city = city.lower()
            external_link = f"https://www.experimmoorta.be/nl/te-huur/{city}/{ext_link}/{ext_id}/"
            item_loader.add_value("external_link", external_link)

            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "habitation" in p_type_string.lower()):
        return "house"
    else:
        return None

   