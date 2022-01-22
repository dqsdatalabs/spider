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
    name = 'everestproperties_be'
    execution_type = 'testing'
    country='belgium'
    locale='fr'
    external_source = "Everest_PySpider_belgium"
    url = "https://www.everestproperties.be/page-data/fr/a-louer/page-data.json"
    # custom_settings = {
    #     "HTTPCACHE_ENABLED": False
    # }


    headers = {
        'authority': 'www.everestproperties.be',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'origin': 'https://www.everestproperties.be',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'accept': '*/*',
        'referer': 'https://www.everestproperties.be/fr/a-louer/',
        'accept-language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
    }
    
    def start_requests(self):
        
        yield Request(
            url=self.url,
            callback=self.parse,
            headers=self.headers
        ) 

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]
        for data in data:
            if data["language"] == "fr":
     
                item_loader = ListingLoader(response=response)
                data1=data['City'].lower().replace("-","")
                data2=str(data["TypeDescription"]).lower().replace(" ","-").replace("é","e").replace("-+-","--").replace(",","").replace("!","").replace("è","").replace("à","a")
                if data2 and "/" in data2:
                    first=data2.split("/")[0].replace("-","")
                    second=data2.split("/")[1]
                    data2=first+second
                data3=data["ID"]
                data2=data2.replace("---","--")

                item_loader.add_value("external_link", f"https://www.everestproperties.be/fr/a-louer/{data1}/{data2}/{data3}")
                if get_p_type_string(data["MainTypeName"]):
                    item_loader.add_value("property_type", get_p_type_string(data["MainTypeName"]))
                else:
                    continue
                item_loader.add_value("external_source", self.external_source)
                
                
                item_loader.add_value("external_id", str(data["ID"]))
                item_loader.add_value("title", data["TypeDescription"])
                
                address = f"{data['Street']} {data['City']} {data['Zip']}"
                item_loader.add_value("address", address)
                item_loader.add_value("city", data['City'])
                item_loader.add_value("zipcode", data['Zip'])
                
                square_meters = data['SurfaceTotal']
                if square_meters and len(str(square_meters))<5:
                    item_loader.add_value("square_meters", data['SurfaceTotal'])

                item_loader.add_value("rent", data['Price'])
                item_loader.add_value("currency", "EUR")
                item_loader.add_value("description", data['DescriptionA'])
                item_loader.add_value("room_count", data['NumberOfBedRooms'])
                item_loader.add_value("bathroom_count", data['NumberOfBathRooms'])
                
                
                item_loader.add_value("latitude", data['GoogleX'])
                item_loader.add_value("longitude", data['GoogleY'])

                images = data["SmallPictures"]
                for i in images:
                    item_loader.add_value("images", i)
                
                if data["HasTerrace"]:
                    item_loader.add_value("terrace", True)
                
                parking = data["NumberOfGarages"]
                if parking and parking !=0:
                    item_loader.add_value("parking", True)
                
                if "EnergyPerformance" in data:
                    item_loader.add_value("energy_label", str(data['EnergyPerformance']))
                
                item_loader.add_value("landlord_name", "Everest Properties")
                item_loader.add_value("landlord_phone", "02/733.70.70")
                item_loader.add_value("landlord_email", "info@everestproperties.be")
                
                yield item_loader.load_item()
         
def get_p_type_string(p_type_string):
    if p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "duplex" in p_type_string.lower() or "triplex" in p_type_string.lower() or "chaussée" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None       