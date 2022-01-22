# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Vester_immobilien_deSpider(Spider):
    name = 'vester_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.vester-immobilien.de"]
    start_urls = ["https://www.vester-immobilien.de/angebotstyp/vermietung/?proptype&location&price&rooms"]
    position = 1

    def parse(self, response):
        for url in response.css("div.propbox a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.pagetitle::text").get()
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
        ):
            return

        rent = response.css("div.propprice::text").get()
        rent = rent.split(",")[0]
        if( not re.search(r"([0-9]{2,})", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"
        
        
        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]

        deposit = response.css("td:contains('Kaution') + td::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        square_meters = response.css("td:contains('Wohnfläche') + td::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]

        room_count = response.css("td:contains('Zimmer') + td::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("td:contains('Badezimmer') + td::text").get()

        address = response.css("div.propaddress p::text").get()
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]


        available_date = response.css("td:contains('Verfügbar ab:') + td::text").get()
        energy_label = response.css("td:contains('Energieeffizienzklasse') + td::text").get()
        balcony = response.css("td:contains('Balkon')").get()
        if(balcony):
            balcony = True
        else:
            balcony = False
       
        description = response.css("div.tabber p::text").getall()
        description = " ".join([description[0], description[1], description[2], description[3], description[4]])
        description = description_cleaner(description)

        images = response.css("a.slider-item::attr(href)").getall()
        external_id = response.css("td:contains('Objekt ID:') + td::text").get()
        
        landlord_name = response.css("span.profilename span[itemprop='name']::text").get()
        landlord_phone = response.css("span[itemprop='telephone'] a::text").get()
        landlord_email = response.css("span a[itemprop='email']::text").get()

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        item_loader.add_value("energy_label", energy_label) # String
        item_loader.add_value("balcony", balcony) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
