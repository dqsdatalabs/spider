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
    name = 'cours_saint_germain_com'
    execution_type='testing'
    country='france'
    locale='fr'    
    external_source = "Cours_Saint_Germain_PySpider_france"
    def start_requests(self):
        start_urls = ["https://cours-saint-germain.com/api/estates/search"]
        payload= {
            'zip_code': '',
            'price_max': '',
            'nb_chambres_min': '',
            'subcategory': '',
            'category': '2',
            '_token': 'XH2OyGVhmgHOTwSkc5ud8YYonLo5lnu5YdMKJfu0',
            'api_key': '8AwZQs9HyPQg9Gzy'
        }

        yield FormRequest(start_urls[0], formdata=payload, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data:
            follow_url = f"https://cours-saint-germain.com/fr/catalogue/location-immobiliere/{item['slug']}"
            yield Request(follow_url, callback=self.populate_item,meta={'item': item})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.xpath("//h1/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item = response.meta.get('item')
        title = "".join(response.xpath("//div[@id='estates-title']//span/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())   
        rent = response.xpath("//div[@id='estates-title']//span[contains(.,'€')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))    
        bathroom_count = response.xpath("substring-after(//div[@id='quartier']//li[contains(.,'Salle de bains')]//text(),':')").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)  
      
        item_loader.add_value("room_count", item["bedrooms"])           
        description = item["comments"]
        item_loader.add_value("description", description.strip())   
          
        if "area_value" in item and item["area_value"]:
            item_loader.add_value("square_meters", int(float(item["area_value"])))   
        if "floor_value" in item:
            item_loader.add_value("floor", str(item["floor_value"]))  
        if "price_fees" in item:
            item_loader.add_value("utilities", int(float(item["price_fees"])))  
        if "price_deposit" in item and item["price_deposit"]:
            item_loader.add_value("deposit", int(float(item["price_deposit"])))  
        city = item["city"]
        zipcode = item["zipcode"]
        longitude = item["longitude"]
        latitude = item["latitude"]
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        if "address" in item:
            address = ""
            if item["address"]:
                if city and zipcode:
                    address = item["address"] +", "+str(city)+", "+zipcode
            else:
                address = city +", "+zipcode           
            item_loader.add_value("address", address) 
        if "subcategory_label" in item:
            if item["subcategory_label"] and "Meublée" in item["subcategory_label"]:
                item_loader.add_value("furnished", True)
        if "services" in item:
            attribute = item["services"]
            if "Ascenseur" in attribute:
                item_loader.add_value("elevator", True)
            if "Lave-linge" in attribute:
                item_loader.add_value("washing_machine", True)
            if "Lave-vaisselle" in attribute:
                item_loader.add_value("dishwasher", True)
            if "Meublé" in attribute:
                item_loader.add_value("furnished", True)
        balcony = response.xpath("//div[@id='quartier']//li[contains(.,'Balcon:')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        energy = item["regulations"]
        for x in energy:
            if x["type"]==1:
                item_loader.add_value("energy_label", energy_label_calculate(x["value"]))

        images = [response.urljoin(x) for x in item["images"]]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "COURS SAINT-GERMAIN")
        item_loader.add_value("landlord_phone", "+33 1 42 38 70 20")    
        item_loader.add_value("landlord_email", "Contact@cours-saint-germain.com")    

        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(float(energy_number))
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None