# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import re
import dateparser

class MySpider(Spider):
    name = 'versailles_cimm_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Versailles_Cimm_PySpider_france'
    custom_settings = {
        'HTTPCACHE_ENABLED':False,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://api.cimm.com/api/realties?operation=location&realty_family=appartement&room_number__gte=&room_number__Lte=&field_surface__gte=&field_surface__lte=&inhabitable_surface__gte=&inhabitable_surface__lte=&price__gte=&price__lte=&fields=id,public_location,price,slug&in_bbox=-2.234248670282935,39.9656633835561,7.455692735967029,51.80276766353921&sold_rented=false&agency=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://api.cimm.com/api/realties?operation=location&realty_family=maison&room_number__gte=&room_number__Lte=&field_surface__gte=&field_surface__lte=&inhabitable_surface__gte=&inhabitable_surface__lte=&price__gte=&price__lte=&fields=id,public_location,price,slug&in_bbox=-2.7946219811875643,40.041406292311365,6.895319425062399,51.86387097241306&sold_rented=false&agency=",
                ],
                "property_type" : "house" 
            },


        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["results"]:
            ext_url = f"https://cimm.com/bien/{item['slug']}"    
            follow_url = f"https://api.cimm.com/api/realties/{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type'], "ext_url":ext_url})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        # item_loader.add_value("external_link",response.url)
 
        if "garage" in response.meta["ext_url"]:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.meta["ext_url"])
        item_loader.add_value("external_source", self.external_source)
        data = json.loads(response.body)

        images = [response.urljoin(x["image"])for x in data["realtyphoto_set"]]
        if images:
                item_loader.add_value("images", images)
        else:
            images  = data["photo"]
            item_loader.add_value("images", images)

        title= data["fr_title"]
        if title:
            item_loader.add_value("title", title)   
        else:
            title=data["operation"]+" "+ data["realtytype"]["name"]+" "+str(data["room_number"])+" " +"pièces"+" "+str(data["inhabitable_surface"])+" "+"m2"
            item_loader.add_value("title", title) 
        item_loader.add_value("description", data["fr_text"])    
      
        item_loader.add_value("landlord_name", data["agency_name"])    

        item_loader.add_value("landlord_name", data["agency_name"])
        if "nego_phone" in data: 
            item_loader.add_value("landlord_phone", data["nego_phone"])    
        else:
            item_loader.add_value("landlord_phone","04 76 31 03 20")    

        item_loader.add_value("external_id", data["reference"])    
        item_loader.add_value("zipcode", data["zipcode"])    
        item_loader.add_value("latitude", data["latitude"])    
        item_loader.add_value("longitude", data["longitude"])    
        item_loader.add_value("rent",data["price"])    
        item_loader.add_value("currency", "EUR")    
        room = data["room_number"]
        if room:
            item_loader.add_value("room_count", room)   
        else:
            item_loader.add_value("room_count", data["bedroom_number"])   
        item_loader.add_value("floor", str(data["floor_number"]))   
        
        if "inhabitable_surface" in data and data["inhabitable_surface"]:
            meters =  str(int(float(data["inhabitable_surface"])))
            if meters:
                if meters !="0":
                    item_loader.add_value("square_meters", meters) 
        if "safety_deposit" in data:
            if data["safety_deposit"]!=None:
                if int(float(data["safety_deposit"])) != 0:
                    dep = int(float(data["safety_deposit"]))
                    item_loader.add_value("deposit",dep ) 

        if data["provisions_for_charges"] !=0:
            if data["safety_deposit"]!=None:
                item_loader.add_value("utilities",str(int(float(data["provisions_for_charges"]))))   

        item_loader.add_value("energy_label",data["dpe_energy_consumption_letter"])    

        furnished =data["furnished"]
        if furnished:
            item_loader.add_value("furnished", True)    
        elif not furnished:
            item_loader.add_value("furnished", False)

        elevator =data["elevator"]
        if elevator:
            item_loader.add_value("elevator", True)    
        elif not elevator:
            item_loader.add_value("elevator", False)
        parking =data["interior_park_number"]
        if parking == 1:
            item_loader.add_value("parking", True)    
        elif parking == 0:
            item_loader.add_value("parking", False)

        terrace =data["terrace"]
        if terrace:
            item_loader.add_value("terrace", True)    
        elif not terrace:
            item_loader.add_value("terrace", False)
            

        city=data["city_name"]
        if city:
            item_loader.add_value("city",city)
        address=data["country"]
        if address and "FR" in address: 
            item_loader.add_value("address","France")
        
                

        yield item_loader.load_item()