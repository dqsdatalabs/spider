# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# disabled
# added to Versailles_Cimm_PySpider_france


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'saint_simeon_bressieux_cimm_disabled'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Saint_Simeon_Bressieux_Cimm_PySpider_france"
    post_urls = ['https://api.cimm.com/api/realties?operation=location&realty_family=&room_number__gte=&room_number__Lte=&field_surface__gte=&field_surface__lte=&inhabitable_surface__gte=&inhabitable_surface__lte=&price__gte=&price__lte=&fields=id,virtual_visit,realtyphoto_set,realty_family,public_location,room_number,inhabitable_surface,field_surface,operation,photo,price,city_name,city_cp,slug,pool,bedroom_number,garage_number,negotiator,topi_admin,compromise,realtytype&in_bbox=4.815096701365922,44.9205011824442,5.717348898631551,45.73535358448491&ordering=&sold_rented=false&compromise=false&agency=176&limit=20&offset=0']  # LEVEL 1

    def start_requests(self):
        yield Request(self.post_urls[0], self.parse)
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data["results"]:
            follow_url = f"https://api.cimm.com/api/realties/{item['id']}"
            if get_p_type_string(item["realtytype"]["name"]):
                yield Request(
                    follow_url, 
                    callback=self.populate_item, 
                    meta={
                        "property_type": get_p_type_string(item["realtytype"]["name"]),
                        "data": item
                    })

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = json.loads(response.body)

        external_link = (data["details_link"])
        item_loader.add_value("external_link", external_link)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("title",data["realtytype"]["name"])

        item_loader.add_value("external_id",str(data["reference"]))
        item_loader.add_value("latitude",str(data["public_location"]["coordinates"][0]))
        item_loader.add_value("longitude",str(data["public_location"]["coordinates"][1]))
        item_loader.add_value("room_count",str(data["room_number"]))
        
        square_meters = (data.get("inhabitable_surface"))
        if square_meters:
            if "." in str(square_meters):
                square_meters = str(square_meters).split(".")[0]
            else:
                square_meters = str(square_meters)
        else:
            square_meters = (data.get("field_surface"))
            if "." in str(square_meters):
                square_meters = str(square_meters).split(".")[0]
            else:
                square_meters = str(square_meters)

        item_loader.add_value("square_meters",square_meters)
        rent = str(data["price"])
        if rent:
            rent = rent.split(".")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        
        city = data["city_name"]
        if city:
            item_loader.add_value("city", city)
        zipcode = data["city_cp"]
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        address = city + " " + zipcode
        if address:
            item_loader.add_value("address", address)
        
        floor = str(data["floor_number"])
        if floor and "None" not in floor:
            item_loader.add_value("floor", floor)

        deposit = data["safety_deposit"]
        if deposit:
            item_loader.add_value("deposit", deposit)
        else:
            deposit = data["fr_text"]
            if deposit and "garantie" in deposit.lower():
                item_loader.add_value("deposit", deposit.split('garantie')[-1].split('€')[0].strip())
        
        energy_label = data["dpe_energy_consumption_letter"]
        if energy_label and "None" not in energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        terrace = data["terrace"]
        if terrace and "None" not in terrace:
            item_loader.add_value("terrace", True)
        
        elevator = data["elevator"]
        if elevator and "None" not in elevator:
            item_loader.add_value("elevator", True)
        
        desc = data["fr_text"]
        if desc:
            item_loader.add_value("description", desc)

        photos = data["realtyphoto_set"]
        images = []
        for photo in photos:
            images.append(photo["image"])
        item_loader.add_value("images",images)
        item_loader.add_value("external_images_count",len(images))   
        
        item_loader.add_value("landlord_name",data["topi_admin"]["first_name"]+data["topi_admin"]["last_name"])
        item_loader.add_value("landlord_email",data["topi_admin"]["email"])
        item_loader.add_value("landlord_phone",data["topi_admin"]["phone_number"])
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None