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
    name = 'propstep_com'
    execution_type = 'testing'
    country = 'denmark' 
    locale ='da'
    custom_settings = {
        # "CONCURRENT_REQUESTS": 3,
        # # "AUTOTHROTTLE_ENABLED": True,
        # # "AUTOTHROTTLE_START_DELAY": .1,
        # # "AUTOTHROTTLE_MAX_DELAY": .3,
        # "DOWNLOAD_DELAY": 3,
        "HTTPCACHE_ENABLED": False,
    }
    url = "https://app.propstep.com/api/search"
  
    def start_requests(self):
        infos = [
            {
                "payload" : {"viewportCoordinates":{"northEast":{},"southWest":{},"center":{}},"radius":None,"transactionType":1,"priceMax":None,"sizeMin":None,"roomsMin":1,"availableFrom":None,"page":1,"pageSize":100,"petsAllowed":False,"handledBy":None},
            }
        ]
        for item in infos:
            yield Request(self.url,
                        method="POST",
                        # headers=self.headers,
                        body=json.dumps(item["payload"]),
                        dont_filter=True,
                        callback=self.parse
                        )
 
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        
        for prop in data["srP"]:
            if prop["properties"]:
                
                for item in prop["properties"]:
                    item_loader = ListingLoader(response=response)
                    prp_type = ""
                    type_val = item["propertyDetails"]["type"]
                    if type_val == 1:
                        prp_type= "apartment"
                    elif type_val == 2:
                        prp_type= "house"
                    else:
                        return
              
                    item_loader.add_value("property_type", prp_type)
                   

                    floor="https://app.propstep.com/api/image/find-public/"+str(item).split("'name': ")[-1].split(",")[0].replace("'","")
                    ext_link = f'https://app.propstep.com/da/boliger/{item["id"]}-{item["slug"]}'
                    ext_id = item["id"]
                    item_loader.add_value("external_source", "Propstep_PySpider_denmark")
                    desc = item["langToDescription"]["da"]
                    
                    title = item["name"]
                    address = item["name"]
                    city = item["location"]["city"]
                    postalcode = item["location"]["postalcode"]
               
                    latitude = item["location"]["point"]["coordinates"][0]
                    longitude = item["location"]["point"]["coordinates"][1]

                    rent = item["transactionDetails"]["price"]
                    deposit = item["transactionDetails"]["deposit"]
                    utilities = item["transactionDetails"]["utilities"]
                    available_date = item["transactionDetails"]["availableFrom"]
                    rooms = item["propertyDetails"]["rooms"]
                    sqm = item["propertyDetails"]["size"]
                    floor = item["propertyDetails"]["floor"]
                    balcony = item["propertyDetails"]["balconies"]  #"balconies":1
                    parking = item["propertyDetails"]["parking"]  #"parking":3
                    terrace = item["propertyDetails"]["terraces"] #"terraces":null
                    pets_allowed = item["propertyDetails"]["petsAllowed"] #"petsAllowed":true
                    # energy_label = item["propertyDetails"]["energyLabel"] #"energyLabel":null
                    washing_machine = item["propertyDetails"]["washingMachine"] #"washingMachine":true
                    dishwasher = item["propertyDetails"]["dryer"] #"dryer":true
                    
                    item_loader.add_value("external_link", ext_link)
                    item_loader.add_value("title", title)
                    item_loader.add_value("description", desc)
                    item_loader.add_value("latitude", str(latitude))
                    item_loader.add_value("longitude", str(longitude))
                    item_loader.add_value("external_id", str(ext_id))
                    item_loader.add_value("zipcode", postalcode)
                    item_loader.add_value("city", city)
                    item_loader.add_value("floor_plan_images",floor)
                    item_loader.add_value("address", address)
                    if rent:
                        item_loader.add_value("rent", str(rent))
                    item_loader.add_value("currency","DKK")
                    if deposit:
                        item_loader.add_value("deposit", str(deposit))
 
                    if utilities:
                        item_loader.add_value("utilities", str(utilities))
                    if sqm:
                        item_loader.add_value("square_meters", str(sqm))
                    if rooms:
                        item_loader.add_value("room_count", str(rooms))
                    if floor:
                        item_loader.add_value("floor", str(floor))
                    # if energy_label:
                    #     item_loader.add_value("energy_label", str(energy_label))

                    if terrace:
                        item_loader.add_value("terrace", True)
                    if balcony:
                        item_loader.add_value("balcony", True)
                    if parking:
                        item_loader.add_value("parking", True)
                    if dishwasher:
                        item_loader.add_value("dishwasher", True)
                    else:
                        item_loader.add_value("dishwasher", False)

                    if washing_machine:
                        item_loader.add_value("washing_machine", True)
                    else:
                        item_loader.add_value("washing_machine", False)
                    if pets_allowed:
                        item_loader.add_value("pets_allowed", True)
                    else:
                        item_loader.add_value("pets_allowed", False)

                    available_date= available_date
                    if available_date:
                        date_parsed = dateparser.parse(available_date, date_formats=["%m-%d-%Y"])
                        if date_parsed:
                            item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

                
                    item_loader.add_value("landlord_name", "Propstep")       
                    item_loader.add_value("landlord_phone", "33 60 55 66") 
                                            
                    images = ["https://app.propstep.com/api/image/find-public/"+x["name"] for x in prop["propertyGroup"]["imagesDefault"]]
                    if images:
                        print(images)
                        item_loader.add_value("images", images)  
                    yield item_loader.load_item()    
    


            

def get_p_type_string(p_type_string):

    if p_type_string and ("lejlighed" in p_type_string.lower() or "lejlighed" in p_type_string.lower()):
        return "apartment"
    if p_type_string and ("rækkehus" in p_type_string.lower() or "tvillingehus" in p_type_string.lower() or "parcelhus" in p_type_string.lower() or "dobbelthus" in p_type_string.lower() or "pyramidehus" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
