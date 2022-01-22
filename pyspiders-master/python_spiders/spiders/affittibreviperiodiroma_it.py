# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Affittibreviperiodiroma_itSpider(Spider):
    name = 'affittibreviperiodiroma_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.affittibreviperiodiroma.it"]
    start_urls = ["https://www.affittibreviperiodiroma.it/"]
    position = 1

    def parse(self, response):
        for url in response.css("ul.sub-menu li.menu-item a::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages)

    def get_pages(self, response):
        for url in response.css("h2.entry-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("a.nextpostslink::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.container h1::text").get()
        rent = response.css("h4:contains('Mensile:') b::text").get()
        rent = str(rent)
        if(not re.search(r"([0-9]+)", rent)):
            return
        currency = "EUR"
        
        room_count = response.css("li.bedrooms::text").getall()
        room_count = "".join(room_count)
        if(not re.search("([1-9])", room_count)):
            room_count = "1"

        square_meters = response.css("li.mq::text").getall()
        square_meters = "".join(square_meters)

        floor = response.css("li.floor::text").getall()
        floor = "".join(floor)
        
        images = response.css("img.opengallery::attr(src)").getall()
        images = [re.sub("-150x150", "", image_src) for image_src in images]

        appliances = response.css("li:contains('Accessori')::text").getall()
        appliances = "".join(appliances)

        washing_machine = "Lavatrice" in appliances
        elevator = "Ascensore" in appliances
        balcony = "balcon" in appliances
        dishwasher = "Lavastoviglie" in appliances
        parking = "Posto Auto" in appliances

        description = response.css("div.entry-content").css("p::text").getall()
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+ [0-9]+ [0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]{3,}', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        latitude = response.css("div.marker::attr(data-lat)").get()
        longitude = response.css("div.marker::attr(data-lng)").get()
        if(latitude and longitude):
            location_data = extract_location_from_coordinates(longitude, latitude)
            address = location_data[2]
            city = location_data[1]
            zipcode = location_data[0]
        else:
            address = response.css("h4.house-location a::text").get()
            city = "Roma"
            zipcode = None

        landlord_name = "affittibreviperiodiroma"
        landlord_phone = "+39 06 44242469"
        landlord_email = "info@affittibreviperiodiroma.it"

        item_loader = ListingLoader(response=response)
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
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
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
