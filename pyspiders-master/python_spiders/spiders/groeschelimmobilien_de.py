# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Groeschelimmobilien_deSpider(Spider):
    name = 'groeschelimmobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.nib.de"]
    start_urls = [
        "https://www.groeschelimmobilien.de/immobilien/?typefilter=1AB70647-4B47-41E2-9571-CA1CA16E0308%7C0",
        "https://www.groeschelimmobilien.de/immobilien/?typefilter=E4DE337C-2DE8-4560-9D5F-1C33A96037B6%7C0"
        ]
    position = 1
    visited_pages = {}

    def parse(self, response):
        for url in response.css("div.estate_list div a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)      

    def populate_item(self, response):
        if(self.visited_pages.get(response.url)):
            return
        self.visited_pages[response.url] = response.url
        property_type = "apartment"

        title = response.css("header h1::text").get()
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
            or "vermietet" in lowered_title
            or "stellplatz" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
        ):
            return

        rent = response.css("td:contains('Miete') + td::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        images = response.css("div.slider_details img::attr(src)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        external_id = response.css("td:contains('Kennung') + td::text").get()
        
        address = response.css("td:contains('Lage') + td::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]        
        zipcode = location_data[0]
        address = zipcode + " " + city       

        room_count = response.css("td:contains('Zimmer') + td::text").get()
        
        square_meters = response.css("td:contains('Wohnfläche') + td::text").get()
        square_meters = re.findall(r"([0-9]+)", square_meters)
        square_meters = ".".join(square_meters)
        square_meters = math.ceil(float(square_meters))

        floor = response.css("td:contains('Etage') + td::text").get()
        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
        
        pets_allowed = response.css("td:contains('Haustiere') + td::text").get()
        if(pets_allowed == "Ja"):
            pets_allowed = True
        else:
            pets_allowed = False
        
        parking = response.css("td:contains('Stellplatzanzahl') + td::text").get()
        if(int(parking) > 0):
            parking = True
        else:
            parking = False

        description = response.css("article:contains('Objektbeschreibung')::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        
        landlord_name = response.css("div.estate_details_contact div.adr div.fn::text").get()
        landlord_phone = response.css("div.estate_details_contact div.adr div.tel a.value::text").get()
        landlord_email = response.css("div.estate_details_contact  a.email::text").get()

        heating_cost = response.css("td:contains('Heizkosten pro Monat') + td::text").get()
        if(heating_cost):
            heating_cost = heating_cost.split(",")[0]

        deposit = response.css("td:contains('Kaution') + td::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("floor", floor) 
        item_loader.add_value("property_type", property_type)  
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("pets_allowed", pets_allowed) 
        item_loader.add_value("parking", parking) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("heating_cost", heating_cost) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
