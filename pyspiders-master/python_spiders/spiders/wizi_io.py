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
    name = 'wizi_io'
    execution_type='testing' 
    country='france'
    locale='fr'
    url = "https://app.wizi.eu/api/public/flats/search.json"
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://desk.wizi.eu',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://desk.wizi.eu/',
        'Accept-Language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        property_types = {"apartment": "{\"offset\":0,\"logement_type\":1}", "house": "{\"offset\":0,\"logement_type\":2}"}
        for k, v in property_types.items():
            yield Request(self.url, 
                            method="POST", 
                            headers=self.headers, 
                            body=v,
                            dont_filter=True, 
                            callback=self.parse, 
                            meta={'property_type': k, 'payload': v})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 15)
        seen = False

        data = json.loads(response.body)
        for item in data:
            seen = True
            follow_url = "https://app.wizi.eu/api/public/flats/" + item["id"] + ".json"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "id":item["id"]})
        
        if page == 15 or seen:
            yield Request(self.url, 
                method="POST", 
                headers=self.headers, 
                body=response.meta["payload"].replace("0", str(page)),
                dont_filter=True, 
                callback=self.parse, 
                meta={'property_type': response.meta["property_type"], 'payload': response.meta["payload"], 'page':page+15})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", "https://desk.wizi.eu/#/app/flat/" + response.meta["id"])
        item_loader.add_value("external_source", "Wizi_PySpider_france")
        data = json.loads(response.body)
        dontlet=data["title"]
        if "garage" in dontlet.lower() or "parking" in dontlet.lower():
            return 
        
        try:
            title = data["title"]
            item_loader.add_value("title",title)
        except: pass
        
        try:
            description = data["description"]
            item_loader.add_value("description",description)
        except: pass

        
        try:
            square_meters = data["surface"]
            item_loader.add_value("square_meters",int(float(square_meters)))
        except: pass
        
        try:
            rent = data["price"]
            item_loader.add_value("rent",rent)
        except: pass
        
        try:
            utilities = data["charges"]
            item_loader.add_value("utilities",utilities)
        except: pass
        
        try:
            floor = data["floors"]
            item_loader.add_value("floor", str(floor))
        except: pass
        
        try:
            room_count = data["bedrooms"]
            item_loader.add_value("room_count",room_count)
        except: pass
        
        try:
            furnished = data["meuble"]
            if furnished:
                item_loader.add_value("furnished", True)
        except: pass
        
        try:
            city = data["city"]
            item_loader.add_value("city",city)
        except: pass
        
        try:
            adress = data["address"]
            item_loader.add_value("address", adress)
        except:
            if "city" in data:
                item_loader.add_value("address", data["city"])
        
        try:
            zipcode = data["postalCode"].split(" - ")[-1]
            item_loader.add_value("zipcode", str(zipcode))
        except: pass

        try:
            latitude = data["latitude"]
            item_loader.add_value("latitude",str(latitude))
        except: pass
        
        try:
            longitude = data["longitude"]
            item_loader.add_value("longitude", str(longitude))
        except: pass
        
        try:
            energy_label = data["dpe"]
            item_loader.add_value("energy_label", energy_label)
        except: pass
        
        try:
            for i in data["documents"]:
                item_loader.add_value("images", f"https://app.wizi.eu/api/document-public/{i['id']}")
        except: pass
         
        name = data["ownedBy"]["name"]
        item_loader.add_value("landlord_name", name)
        
    
        item_loader.add_value("landlord_phone", "01 76 44 06 66")
        item_loader.add_value("landlord_email", "hello@wizi.eu")

        follow_url = "https://app.wizi.eu/api/public/flatsdescription/" + response.meta["id"] + ".json"
        yield Request(follow_url, callback=self.get_details, meta={"item_loader":item_loader})
    
    def get_details(self, response):
        item_loader = response.meta["item_loader"]

        data = json.loads(response.body)
        if data["id"] == "72052":
            return
        
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])
        
        terrace = data["terrace"]
        if terrace and terrace != 0:
            item_loader.add_value("terrace", True)
            
        balcony = data["balcony"]
        if balcony and balcony != 0:
            item_loader.add_value("balcony", True)
            
        garage = data["garage"]
        if garage and garage != 0:
            item_loader.add_value("parking", True)
            
        parking = data["parking"]
        if parking and parking != 0:
            item_loader.add_value("parking", True)
            
        swimming_pool = data["swimming_pool"]
        if swimming_pool and swimming_pool != 0:
            item_loader.add_value("swimming_pool", True)
            
        elevator = data["elevator"]
        if elevator and elevator != 0:
            item_loader.add_value("elevator", True)
            
        washer = data["washer"]
        if washer and washer != 0:
            item_loader.add_value("washing_machine", True)
            
        dishwasher = data["dishwasher"]
        if dishwasher and dishwasher != 0:
            item_loader.add_value("dishwasher", True)

        yield item_loader.load_item()