# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'mcmakelaardij_nl'   
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source = 'Mcmakelaardij_PySpider_netherlands'
    start_urls = ['https://back-end.mcmakelaardij.nl/api/house'] 
    # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        
        for item in data:
            if item["catergory"] and "huur" in item["catergory"]:
                prop = item["type"] 
                if prop == "Appartement":
                    prop_type = "apartment"
                elif prop == "Kamer":
                    prop_type = "apartment"
                else:
                    continue
                
                external_id = item["house_id"]
                base_url = "https://mcmakelaardij.nl/Single/"
                follow_url = base_url + str(external_id)
                lat = item["lat_coord"]
                lng = item["long_coord"]
                rent = item["prijs"]
                address = item["adres"]
                city = item["stad"]
                room = item["bedrooms"]
                bathroom = item["bathroom"]
                squ = item["meters"]
                desc = item["bijzonderheden"]           
                
                if item["images"]:
                    result_img = []
                    for img in item["images"]:
                        result_img.append("https://back-end.mcmakelaardij.nl/static/"+ img["filename"])
      
                yield Request(follow_url, 
                    dont_filter=True, 
                    callback=self.populate_item, 
                    meta={'external_id': external_id, 'lat':lat, 'lng':lng, 'rent': rent, 'address': address, 'city': city, 'room': room, 'bathroom': bathroom, 'squ': squ, 'desc': desc, 'result_img':result_img, 'prop_type': prop_type}
                )
                 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("prop_type"))
        external_id = response.meta.get("external_id")
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        address = response.meta.get("address")
        city = response.meta.get("city") 
        rent = response.meta.get("rent")     
        room_count = response.meta.get("room")
        bathroom_count = response.meta.get("bathroom")
        square_meters = response.meta.get("squ")   
        description = response.meta.get("desc")
        images = response.meta.get("result_img")

        if external_id:
            item_loader.add_value("external_id", str(external_id))

        if address:
            item_loader.add_value("title", address)
        
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency","EUR")
        
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        if square_meters:
            item_loader.add_value("square_meters", square_meters)
   
        if address:
            item_loader.add_value("address", address)
        
        if city:
            item_loader.add_value("city", city)

        if lat:
            item_loader.add_value("latitude", str(lat))
        
        if lng:
            item_loader.add_value("longitude", str(lng))

        if description:
            item_loader.add_value("description", description)

        
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "MC Makelaardij")
        item_loader.add_value("landlord_phone", "085-130 44 50")
        item_loader.add_value("landlord_email", "info@mcmakelaardij.nl")
        
        yield item_loader.load_item()