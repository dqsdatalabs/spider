# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Suche_vivawest_deSpider(Spider):
    name = 'suche_vivawest_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.suche.vivawest.de"]
    start_urls = ["https://suche.vivawest.de/"]
    position = 1

    def parse(self, response):
        for url in response.css("a[rel='property-detail']::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("li.next a::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div#property-detail h1::text").get()
        lowered_title = title.lower()
        if(
            "gewerbe" in lowered_title
            or "gewerbefläche" in lowered_title
            or "büro" in lowered_title
            or "praxisflächen" in lowered_title
            or "ladenlokal" in lowered_title
            or "arbeiten" in lowered_title 
            or "gewerbeeinheit" in lowered_title
        ):
            return
        rent = response.css("div:contains('Kaltmiete') strong::text").get()
        if( not rent ):
            return
        
        rent = rent.split(",")[0]
        currency = "EUR"

        square_meters = response.css("dt:contains('Wohnfläche') + dd::text").get()
        room_count = response.css("dt:contains('Zimmer') + dd::text").get()

        address = response.css("a.map-link::text").get()

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        images = response.css("img.sp-image::attr(data-large)").getall()

        images = [ response.urljoin(image_src) for image_src in images]

        external_id = response.css("dt:contains('Expose-Nr') + dd::text").get()
        if(external_id):
            external_id = external_id.strip()
             
        available_date = response.css("dt:contains('Bezugsfrei ab') + dd::text").get() 
        if(available_date):
            available_date = available_date.strip()
        
        utilities = response.css("dt:contains('Nebenkosten') + dd::text").get()        
        if(utilities):
            utilities = utilities.split(",")[0]
        
        
        heating_cost = response.css("dt:contains('Heizkosten') + dd::text").get()        
        if(heating_cost):
            heating_cost = heating_cost.split(",")[0]
        
        deposit = response.css("dt:contains('Kaution') + dd::text").get()        
        if(deposit):
            deposit = deposit.split(",")[0]
            deposit = deposit.split(".")
            deposit = "".join(deposit)

        energy_label = response.css("p:contains('Energie­effizienz­klasse:')::text").get()
       
        description = response.css("div.accordion p::text").getall()
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]{3,}', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        amenities = response.css("div.accordion ul li::text").getall()

        amenities = " ".join(amenities)
        elevator = "Aufzug" in amenities
        balcony = "Balkon" in amenities

        landlord_name = response.css("div.contact-box::text").getall()[1].strip()
        landlord_phone = response.css("div.contact-box a::text").get()

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
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
