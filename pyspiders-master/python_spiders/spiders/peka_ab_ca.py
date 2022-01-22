# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class Peka_ab_caSpider(Spider):
    name = 'peka_ab_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.peka.ab.ca"]
    start_urls = ["https://peka.ab.ca/canmore-rentals"]

    def parse(self, response):
        for url in response.css("div.field-content a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):

        property_type = response.css("div.field-label:contains('Type:') + div.field-items div.field-item::text").get()
        title = response.css("div.field-item::text").get()
        if("house" in property_type.lower()):
            property_type = "house"
        else:
            property_type = "apartment"
        
        description = response.css("div.field-type-text-with-summary div.field-items div.field-item p::text").getall()
        description = " ".join(description)

        rent = response.css("div.field-label:contains('Rent:') + div.field-items div.field-item::text").get()
        rent = re.findall("([0-9]+)", rent)[0]
        currency = "CAD"
        external_id = response.css("div.field-label:contains('Booking Id:') + div.field-items div.field-item::text").get()
        
        room_count = response.css("div.field-label:contains('Bedrooms:') + div.field-items div.field-item::text").get()
        room_count = str(eval(room_count))

        bathroom_count = response.css("div.field-label:contains('Bathrooms:') + div.field-items div.field-item::text").get()
        bathroom_count = str(int(float(bathroom_count)))
        
        available_date = response.css("div.field-label:contains('Available:') + div.field-items div.field-item span::text").get()
        if(available_date == None):
            available_date = response.css("div.field-label:contains('Available:') + div.field-items div.field-item::text").get()

        images = response.css("img.img-responsive::attr(src)").getall()

        pets_allowed = response.css("div.field-label:contains('Pets Allowed:') + div.field-items div.field-item::text").get()
        if(pets_allowed == "No"):
            pets_allowed = False
        else:
            pets_allowed = True
        
        elevator = response.css("div.field-label:contains('Elevator:') + div.field-items div.field-item::text").get()
        if(elevator == "No"):
            elevator = False
        else:
            elevator = True
        
        parking = response.css("div.field-label:contains('Parking:') + div.field-items div.field-item::text").get()
        if(parking):
            parking = True
        else:
            parking = False
        
        location_script = response.css("script:contains('\"lat\"')::text").get()
        latitude = re.findall('"lat":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        longitude = re.findall('"lng":"(-?[0-9]+\.[0-9]+)"', location_script)[0]
        
        landlord_name = 'peka'
        landlord_phone = "403 454 3050"
        landlord_email = "service@peka.ca"

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("images", images)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("parking", parking)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
       
        yield item_loader.load_item()
