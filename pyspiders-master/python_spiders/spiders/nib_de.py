# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Nib_deSpider(Spider):
    name = 'nib_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.nib.de"]
    start_urls = ["https://www.nib.de/immobilien/mieten"]
    position = 1

    def parse(self, response):
        for url in response.css("a.btn-details:contains('zum Exposé')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a[rel='next']::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h2.first::text").get()
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
            or "stellplätze" in lowered_title
            or "garage" in lowered_title
            or "restaurant" in lowered_title
            or "lager" in lowered_title
            or "einzelhandel" in lowered_title
            or "sonstige" in lowered_title
            or "grundstück" in lowered_title
            or "verkauf" in lowered_title
            or "reserviert" in lowered_title
            or "ladenfläche" in lowered_title
            or "halle" in lowered_title
            or "hallen" in lowered_title
            or "geschäftshaus" in lowered_title
            or "gaststätte" in lowered_title
            or "arzt" in lowered_title
        ):
            return

        rent = response.css("p.priceinfo span.price::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        
        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        deposit = response.css("td:contains('Kaution') + td::text").get()


        square_meters = response.css("p.sizeinfo span.size::text").get()
        if(not square_meters):
            square_meters = response.css("td:contains('Ladenfläche') + td::text").get()
        if(square_meters):    
            square_meters = re.findall("([0-9]+)", square_meters)
            square_meters = "".join(square_meters)

        room_count = response.css("p.roominfo span.rooms::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        city = response.css("p.position span.city::text").get()
        location_data = extract_location_from_address(city)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]


        available_date = response.css("td:contains('verfügbar ab') + td::text").get()
        energy_label = response.css("td:contains('Energieeffizienzklasse') + td::text").get()
        description = response.css("div.panel-body::text").getall()
        description = " ".join(description)

        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        images = response.css("li.img a::attr(href)").getall()
        if(len(images) < 1):
            images = response.css("a[rel='immogallery']").getall()

        images = [ response.urljoin(image_src) for image_src in images]
        external_id = response.url.split("/")[-1]
        contacts = response.css("h4:contains('Ihr Ansprechpartner') + p::text").getall()
        contacts = " ".join(contacts)

        landlord_name = contacts.split("\n")[0]
        if(not landlord_name):
            landlord_name = "nib"
        try:
            landlord_phone = contacts.split("Telefon:")[1].split("\n")[0]
        except:
            contacts = response.css("div.provider").css("p::text").getall()
            contacts = " ".join(contacts)
            landlord_phone = contacts.split("Telefon:")[1].split("\n")[0]

        landlord_email = "iser@nib.de"

        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
