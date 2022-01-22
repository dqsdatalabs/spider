# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.user_agents import random_user_agent

from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Jaspert_immobilie_deSpider(Spider):
    name = 'jaspert_immobilie_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.jaspert-immobilien.de"]
    position = 1
    custom_settings = {
        "User-Agent": random_user_agent()

    }
    pages_visited = {}
    

    def start_requests(self):
        pages = [1,2]
        for page in pages:            
            yield FormRequest(url=f"https://www.ivd24immobilien.de/objektlisten/index.php",
                        callback=self.parse,
                        formdata = {"vermarktungsart_id":"10000000010", 
                        "filter": "1", 
                        "oid": "366", 
                        "sid":"cbn1or74pppuhljs0kodskil82"},
                        method='POST')

    def parse(self, response):
        for url in response.css("a:contains('zum Exposé')::attr(data-href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
             
        next_page = response.css("li a::attr(data-href)")[-1].get()
        if (next_page not in self.pages_visited):
            self.pages_visited[next_page] = next_page
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)


    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("h3::text").get()
        if( "Vermietet" in title):
            return
        
        rent = response.css("div:contains('Miete') span::text").get()
        if( not re.search("([0-9]+)", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        
        currency = "EUR"

        room_count = response.css("div.ivd24-infobox:contains('Zimmer') span::text").get()
        if("," in room_count):
            room_count = room_count.split(",")[0]

        square_meters = response.css("div.ivd24-infobox:contains('Wohnfläche') span::text").get()
        square_meters = re.findall("([0-9]+)", square_meters)
        square_meters = "".join(square_meters)
        
        external_id = response.css("dt:contains('ivd24 Objektnummer') + dd::text").get() 

        address = response.css("dt:contains('Objektanschrift') + dd::text").get() 
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        utilities = response.css("dt:contains('Nebenkosten') + dd::text").get() 
        if utilities:
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)

        deposit = response.css("dt:contains('Kaution') + dd::text").get() 
        deposit = re.findall("([0-9]+)", deposit)
        deposit = "".join(deposit)

        floor = response.css("dt:contains('Etage') + dd::text").get()
        bathroom_count = response.css("dt:contains('Anzahl Badezimmer') + dd::text").get()
        available_date = response.css("dt:contains('Verfügbar ab') + dd::text").get()

        approved_imaged = "https://www.ivd24immobilien.de/images/ivd24immobilien_hacken_gruen.png"
        pets_allowed = response.css("dt.ivd24-normal:contains('Haustiere erlaubt') + dd img::attr(src)").get()
        if pets_allowed:
            if(approved_imaged in pets_allowed ):
                pets_allowed = True
            else:
                pets_allowed = False
        
        parking = response.css("dt.ivd24-normal:contains('Anzahl Stellplätze') + dd::text").get()
        if(parking):
            parking = True
        else: 
            parking = False

        description = response.css("h4:contains('Objektbeschreibung') + p::text").getall()
        description = " ".join(description)

        images = response.css("div#ivd24-expose-thumbnails img::attr(src)").getall()

        images = [re.sub("thumb", "web", image_src) for image_src in images]

        landlord_name = "jaspert-immobilien"
        landlord_phone = "0711 9183396"
        landlord_email = "info@jaspert-immobilien.de"

        oid = re.findall("oid=([0-9]+)", response.url)[0]
        id2 = re.findall("&id=([0-9]+)", response.url)[0]
        url = f"https://www.jaspert-immobilien.de/immobilien/#{oid}-{id2}"

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", url) # String
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
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        # # # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # # # #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # # # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # # #item_loader.add_value("water_cost", water_cost) # Int
        # # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
