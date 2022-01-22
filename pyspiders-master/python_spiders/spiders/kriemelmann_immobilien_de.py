# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Kriemelmann_immobilien_deSpider(Spider):
    name = 'kriemelmann_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.kriemelmann-immobilien.de"]
    start_urls = ["https://kriemelmann-immobilien.de/miete"]
    position = 1

    def parse(self, response):
        for url in response.css("a.ws-clickthis::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        rented_badge = response.css("div.overflow-hidden span.label::text").get()
        if(rented_badge == "Vermietet" or rented_badge == "Reserviert"):
            return

        title = response.css("header.header-immobilie h1::text").get()
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

        cold_rent = response.css("div.details div.price span:contains('Kaltmiete') ~ strong::text").get()
        warm_rent = response.css("div.details div.price span:contains('Warmmiete') ~ strong::text").get()

        rent = None
        if( not cold_rent ):
            cold_rent = "0"
        
        if( not warm_rent):
            warm_rent = "0"

        cold_rent = cold_rent.split(",")[0]
        warm_rent = warm_rent.split(",")[0]
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not re.search(r"([0-9]{2,})", rent)):
            return

        currency = "EUR"
        
        utilities = response.css("span:contains('Nebenkosten')::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall(r"([0-9]+)", utilities)[0]

        square_meters = response.css("li.column:contains('m²')::text").getall()
        square_meters = " ".join(square_meters)
        if(square_meters):    
            square_meters = square_meters.split(",")[0]

        room_count = response.css("li.column:contains('Zimmer')::text").getall()
        room_count = " ".join(room_count).strip()
        try:
            room_count = re.findall(r"([1-9])", room_count)
            room_count = ".".join(room_count)
        except:
            room_count = "1"

        room_count = str(math.ceil(float(room_count)))
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("li.column:contains('Badezimmer')::text").getall()
        bathroom_count = " ".join(bathroom_count)
        try:
            bathroom_count = re.findall("([1-9])", bathroom_count)
            bathroom_count = ".".join(bathroom_count)
        except:
            bathroom_count = "1"

        energy_label = response.css("p:contains('Energieeffizienzklasse')::text").getall()
        energy_label = " ".join(energy_label)
        energy_label = re.findall(r"Energieeffizienzklasse:\s+([A-Z])", energy_label)
        if(len(energy_label) > 0):
            energy_label = energy_label[0]
        else: 
            energy_label = None
        
        city = response.css("ul.breadcrumbs li:contains('in')::text").getall()
        city = " ".join(city)
        city = city.split("in")[1]
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        description = response.css("p.lead::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        images = response.css("figure.item a::attr(href)").getall()

        external_id = response.css("p.obj-nr span::text").get()
        
        landlord_name = response.css("div.contact strong::text").get()
        landlord_phone = response.css("div.contact span::text").getall()
        landlord_phone = " ".join(landlord_phone)
        landlord_phone = landlord_phone.split("Tel.")[1]
        landlord_email = "info@kriemelmann-immobilien.de"

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
        item_loader.add_value("property_type", property_type) 
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 
        item_loader.add_value("bathroom_count", bathroom_count) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent", rent) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
