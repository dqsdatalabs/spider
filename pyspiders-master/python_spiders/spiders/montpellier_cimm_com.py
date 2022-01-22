# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
 
 # disabled
 # added to Versailles_Cimm_PySpider_france
 
class MySpider(Spider):
    name = 'montpellier_cimm_com_disabled'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Montpellier_cimm_com_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://api.cimm.com/api/realties?operation=location&realty_family=maison&room_number__gte=&room_number__Lte=&field_surface__gte=&field_surface__lte=&inhabitable_surface__gte=&inhabitable_surface__lte=&price__gte=&price__lte=&fields=id,virtual_visit,realtyphoto_set,realty_family,public_location,room_number,inhabitable_surface,field_surface,operation,photo,price,city_name,city_cp,slug,pool,bedroom_number,garage_number,negotiator,topi_admin,compromise&in_radius=3.90117470000000,43.60472750000000,20&sold_rented=false&compromise=false&agency=&limit=20&offset=0", "property_type": "house"},
            {"url": "https://api.cimm.com/api/realties?operation=location&realty_family=appartement&room_number__gte=&room_number__Lte=&field_surface__gte=&field_surface__lte=&inhabitable_surface__gte=&inhabitable_surface__lte=&price__gte=&price__lte=&fields=id,virtual_visit,realtyphoto_set,realty_family,public_location,room_number,inhabitable_surface,field_surface,operation,photo,price,city_name,city_cp,slug,pool,bedroom_number,garage_number,negotiator,topi_admin,compromise&in_radius=3.90117470000000,43.60472750000000,20&sold_rented=false&compromise=false&agency=&limit=20&offset=0", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        data = json.loads(response.body)["results"]        
        for item in data:
            follow_url = f"https://api.cimm.com/api/realties/{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
             
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = json.loads(response.body)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", data["details_link"])
        city = data["city_name"]
        zipcode = data["zipcode"]
        agency_name = data["agency_name"]
        external_id = data["reference"]
        latitude = data["latitude"]
        longitude = data["longitude"]
        price = data["price"]
        bedroom = data["bedroom_number"]
        room = data["room_number"]
        floor = data["floor_number"]
        furnished = data["furnished"]
        elevator = data["elevator"]
        terrace = data["terrace"]
        pool = data["pool"]
        title = data["fr_title"]
        description = data["fr_text"]  

        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)

        images = [x["image"] for x in data["realtyphoto_set"]]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        if zipcode:
            item_loader.add_value("address", city+" "+zipcode)
        else:
            item_loader.add_value("address", city)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")
        


        if room != 0:
            item_loader.add_value("room_count", room)
        elif bedroom != 0:
            item_loader.add_value("room_count", bedroom)
        
        if "inhabitable_surface" in data:
            item_loader.add_value("square_meters", int(float( data["inhabitable_surface"])))

        if "dpe_energy_consumption_letter" in data:
            item_loader.add_value("energy_label", data["dpe_energy_consumption_letter"])
        if furnished:
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        if terrace:
            item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace",False)
        if elevator:
            item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)
        if pool:
            item_loader.add_value("swimming_pool",True)
        else:
            item_loader.add_value("swimming_pool",False)


        item_loader.add_value("floor", str(floor))
        item_loader.add_value("landlord_name", "CIMM IMMOBILIER MONTPELLIER")

        email= "montpellier@cimm.com"
        item_loader.add_value("landlord_email", email)
        phone = "04 67 68 13 93"
        item_loader.add_value("landlord_phone", phone)

        yield item_loader.load_item()