# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import urllib
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Gesobau_deSpider(Spider):
    name = 'btg24_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.btg24.de"]
    start_urls = ["https://www.btg24.de/wohnungen"]
    position = 1

    def parse(self, response):
        for url in response.css("a.offer-details::attr(href)").getall():
            yield Request(response.urljoin(url), callback = self.populate_item, dont_filter = True)

    
    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("div.description h3::text").get()
        description = title
        rent = response.css("td:contains('Kaltmiete (zzgl. NK)') + td strong::text").get()
        rent = rent.split(",")[0]
        currency = "EUR"

        utilities = response.css("td:contains('Nebenkosten') + td strong::text").get()
        utilities = utilities.split(",")[0]

        deposit = response.css("td:contains('Kaution') + td strong::text").get()
        deposit = deposit.split(",")[0]

        external_id = response.css("td:contains('ID') + td strong::text").get()
        location = response.css("td:contains('Ort') + td strong::text").get()
        floor = response.css("td:contains('Etage/Lage') + td strong::text").get()

        furnishing = response.css("div.furnishing ul li::text").getall()
        furnishing = " ".join(furnishing)

        elevator = "Aufzug" in furnishing
        balcony = "Balkon" in furnishing
        parking = "Stellplatz" in furnishing

        address_script = response.css("script:contains('var address')::text").get()
        address = re.findall('var address = "(.+)";', address_script)[0]

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        room_count = response.css("td:contains('Räume') + td strong::text").get()

        images = response.css("div.print-img").css("img::attr(src)").getall()
        images = [response.urljoin(urllib.parse.quote(image_src)) for image_src in images]

        landlord_name = "BTG Immobiliare service"
        landlord_phone = "0365 82318-0"
        landlord_email = "info@BTG24.de"
        square_meters = response.css("td:contains('Wohnfläche (ca.)') + td strong::text").get()
        square_meters = re.findall("([0-9]+)", square_meters)
        square_meters = '.'.join(square_meters)
        square_meters = math.ceil(float(square_meters))

        item_loader = ListingLoader(response=response)
        # # MetaData
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
        item_loader.add_value("parking", parking) # Boolean
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
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
