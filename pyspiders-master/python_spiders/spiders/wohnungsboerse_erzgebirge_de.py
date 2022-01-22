# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gesobau_deSpider(Spider):
    name = 'wohnungsboerse_erzgebirge_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.wohnungsboerse-erzgebirge.de"]
    start_urls = ["https://wohnungsboerse-erzgebirge.de/wohnung-mieten/wohnung-nach-ort"]
    position = 1

    def parse(self, response):
        for url in response.css("a.card-footer::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages, dont_filter = True)

    def get_pages(self, response):
        for url in response.css("h3 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("a[rel='next']::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.listing-title-bar h3::text").get()
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

        rent = response.css("div:contains('miete')::text").getall()
        rent = "".join(rent)
        rent = rent.split(",")[0].strip()
        rent = re.findall(r"([0-9]+)", rent)
        rent = "".join(rent)
        if(not re.search(r"([0-9]+)", rent)):
            return
        
        currency = "EUR"
        
        utilities = response.css("div:contains('Nebenkosten')::text").getall()
        utilities = "".join(utilities)
        utilities = utilities.split(",")[0].strip()

        external_id = response.css("div:contains('Nummer')::text").getall()
        external_id = "".join(external_id).strip()

        square_meters = response.css("div:contains('Fläche')::text").getall()
        square_meters = "".join(square_meters).strip()
        square_meters = square_meters.split(",")[0].strip()

        address = response.css("div.col-12:contains('Ort')::text").getall()
        address = "".join(address).strip()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        room_count = response.css("span:contains('Räume') + span.det::text").get()
        if(not room_count):
            room_count = "1"
        floor = response.css("li:contains('Etage') span.det::text").get()

        images = response.css("li.splide__slide img::attr(src)").getall()
        description = response.css("div.details::text").getall()
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)
        
        balcony = "Balkon" in description

        energy_label = response.css("p:contains('Energieeffizienzklasse') span.det::text").get()

        landlord_name = response.css("h4.author__title::text").get()
        landlord_phone = response.css("span.la-phone + span a::text").get()
        landlord_email = response.css("span.la-envelope-o + span a::text").get()

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # item_loader.add_value("furnished", furnished) # Boolean
        # # item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
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
        # item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
