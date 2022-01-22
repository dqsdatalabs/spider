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

class MySpider(Spider):
    name = 'arialiving_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            "https://api.re.vertos.site/listings/site/4e340b0150fb46d59f07d1abb031c4e9",
            "https://api.re.vertos.site/listings/site/5a5081e346554b0991b7bccc4bb22a98",
            "https://api.re.vertos.site/listings/site/f2c3c456feab434ea9a7ac89f3633d16",
            "https://api.re.vertos.site/listings/site/1e08e152670d42b5973bcd0f84365cee",
            "https://api.re.vertos.site/listings/site/5a5e13057beb4e269a04b8c5a4d1b323",
            "https://api.re.vertos.site/listings/site/c5f5db58e69f47b7996e2d86f21f0c75",
            "https://api.re.vertos.site/listings/site/c4c82b605bf8472e9803851f2fa5afaa",
            "https://api.re.vertos.site/listings/site/e2d7105d70ab4fc5be182409332003b3",
        ]
        for start_url in start_urls: yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["listings"]:
            follow_url = "https://listings.re.vertos.site/listing/" + item["site_id"] + "/" + item["listing_id"]
            property_type = item["category"]
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type), "item":item})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Arialiving_Com_PySpider_australia")
        
        item = response.meta.get('item')

        title = item["headline"]
        item_loader.add_value("title", title)
        
        street = item["address"]["street"]
        postcode = item["address"]["postcode"]
        suburb = item["address"]["suburb"]
        item_loader.add_value("address", street+" "+suburb+" "+postcode)
        item_loader.add_value("city", suburb)
        item_loader.add_value("zipcode", postcode)
                
        bathrooms = item["features"]["bathrooms"]
        item_loader.add_value("bathroom_count", bathrooms)
        
        room_count = item["features"]["bedrooms"]
        item_loader.add_value("room_count", room_count)
        
        rent = item["rent"]["amount"]
        item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "USD")
        
        # deposit = item["rent"]["bond"]
        # item_loader.add_value("deposit", deposit)
        
        garages = item["features"]["garages"]
        if garages and garages != 0 :
            item_loader.add_value("parking", True)
            
        pets = item["allowances"]["pets"]
        if pets:
            item_loader.add_value("pets_allowed", True)
            
        furnished = item["allowances"]["furnished"]
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        dateAvailable = item["dateAvailable"]
        date_parsed = dateparser.parse(dateAvailable, date_formats=["%d/%m/%Y"])
        if date_parsed:
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
            
        description = item["description"]
        if description:
            description = re.sub('\s{2,}', ' ', description)
            item_loader.add_value("description", description)
        
        if "m2" in description:
            square_meters = description.split("m2")[0].strip().split(" ")[-1]
            if square_meters !='4':
                item_loader.add_value("square_meters", square_meters)
        
        try:
            floorplans = item["media"]["floorplans"]
            item_loader.add_value("floor_plan_images", str(floorplans).split("'")[3])
            images = [str(x).split("src': '")[1].split("'")[0] for x in item["media"]["images"]]
            if images:
                item_loader.add_value("images", images)
        except: pass
        
        try:
            balcony = item["features"]["balcony"]
            item_loader.add_value("balcony", True)
        except: pass
        
        try:
            dishwasher = item["features"]["dishwasher"]
            item_loader.add_value("dishwasher", True)
        except: pass
        
        item_loader.add_value("landlord_name", "ARIA LIVING")
        item_loader.add_value("landlord_phone", "0499 033 280")
        item_loader.add_value("landlord_email", "rentals@arialiving.com.au")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None