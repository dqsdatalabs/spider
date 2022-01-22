# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Apartments_b2b_comSpider(Spider):
    name = 'apartments_b2b_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.apartments-b2b.com"]
    start_urls = ["https://apartments-b2b.com/"]
    position = 1

    def parse(self, response):
        for url in response.css("a:contains('Umgebung')::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.get_pages, dont_filter = True)

    def get_pages(self, response):
        for url in response.css("h2.tm-property__title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("div.nav-links a.next::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.get_pages, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.entry-title::text").get()
        rent = response.css("span.tm-property__price-value::text").get()
        rent = rent.split(",")
        if(len(rent) > 0):
            rent = rent[0]
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
                
        currency = "EUR"

        description1 = response.css("div.tm-property__description ul li::text").getall()
        description1 = " ".join(description1)

        description2 = response.css("div.tm-property__description p::text").getall()
        description2 = " ".join(description2)

        description = description1 + description2
        external_id = response.url.split("=")[1]
        address = response.css("dt:contains('Standort:') + dd p::text").getall()
        address = " ".join(address)
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        available_date = response.css("dt:contains('Frei ab:') + dd::text").get()
        square_meters = response.css("dt:contains('Größe:') + dd::text").get()
        room_count = response.css("dt:contains('Zimmer:') + dd::text").get()
        room_count = int(float(room_count))

        balcony = response.css("dt:contains('Balkon / Terrasse:') + dd span::text").get()
        if("ja" in balcony):
            balcony = True
            terrace = True
        else: 
            balcony = False
            terrace = False

        parking = response.css("dt:contains('Stellplatz:') + dd span::text").get()
        if( parking):
            if("ja" in parking):
                parking = True
        else:
            parking = False
        amenities = response.css("dt:contains('Ausstattung:') + dd ul li::text").getall()
        amenities = " ".join(amenities).lower()

        furnished = "möbliert" in amenities
        washing_machine = "waschmaschine" in amenities
        dishwasher = "geschirrspüler" in amenities

        images = response.css("div.tm-property-gallery__item img::attr(src)").getall()

        landlord_name = "apartments-b2b"
        landlord_phone = "+49-221-2584000"
        landlord_email = "t.zierenberg@apartments-b2b.com"

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

        # Images  
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # #item_loader.add_value("water_cost", water_cost) # Int
        # #item_loader.add_value("heating_cost", heating_cost) # Int

        # #item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
