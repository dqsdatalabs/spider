# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Jiranek_immobilien_deSpider(Spider):
    name = 'jiranek_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.jiranek-immobilien.de"]
    start_urls = ["https://jiranek-immobilien.de/mieten.html"]
    position = 1

    def parse(self, response):
        for url in response.css("a.button_more::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.row h1::text").get()
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

        cold_rent = response.css("div.label:contains('Kaltmiete:') + div.feature_c::text").get()
        cold_rent = cold_rent.split(",")[0]

        warm_rent = response.css("div.label:contains('Warmmiete:') + div.feature_c::text").get()
        warm_rent = warm_rent.split(",")[0]

        rent = None
        if( not re.search(r"([0-9]+)", cold_rent)):
            cold_rent = "0"
        
        if( not re.search(r"([0-9]+)", warm_rent)):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int(warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        
        if(not rent):
            return
        
        currency = "EUR"
        
        external_id = response.css("div.label:contains('Objektnummer:') + div.feature_c::text").get()
        
        utilities = response.css("div.label:contains('Nebenkosten:') + div.feature_c::text").get()
        utilities = utilities.split(",")[0]

        deposit = response.css("div.label:contains('Kaution:') + div.feature_c::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        square_meters = response.css("div.label:contains('Wohnfläche:') + div.feature_c::text").get()
        square_meters = re.findall(r"([0-9]+)", square_meters)
        square_meters = ".".join(square_meters)
        square_meters = str(math.ceil(float(square_meters)))

        room_count = response.css("div.label:contains('Anzahl Zimmer:') + div.feature_c::text").get()
        available_date = response.css("div.label:contains('Verfügbar ab:') + div.feature_c::text").get()
        energy_label = response.css("div.label:contains('Wertklasse:') + div.feature_c::text").get()
        floor = response.css("div.label:contains('Etage:') + div.feature_c::text").get()
          
        address = response.css("div.label:contains('PLZ, Ort:') + div.feature_c::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]
        address = zipcode + " " + city

        images = response.css("div.listimage img::attr(src)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        description = response.css("div.description div.feature div.feature_c::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        landlord_name = "jiranek-immobilien"
        landlord_phone = "+49 981 3098"
        landlord_email = "info@jiranek-immobilien.de"

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

        item_loader.add_value("available_date", available_date) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("deposit", deposit) 
        item_loader.add_value("utilities", utilities) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("energy_label", energy_label) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 
        item_loader.add_value("landlord_email", landlord_email) 

        yield item_loader.load_item()
