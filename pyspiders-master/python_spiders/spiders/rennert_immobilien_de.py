# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Rennert_immobilien_deSpider(Spider):
    name = 'rennert_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.rennert-immobilien.de"]
    start_urls = ["https://rennert-immobilien.de/immobilien-vermarktungsart/miete/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.property-actions a.btn:contains('Exposé')::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.property-title::text").get()
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

        cold_rent = response.css("div.dt:contains('Kaltmiete') + div.dd::text").get()
        if(cold_rent):
            cold_rent = cold_rent.split(",")[0]

        warm_rent = response.css("div.dt:contains('Warmmiete') + div.dd::text").get()
        if(warm_rent):
            warm_rent = warm_rent.split(",")[0]

        rent = None
        if( not cold_rent ):
            cold_rent = "0"
    
        if( not warm_rent):
            warm_rent = "0"

        cold_rent = re.findall(r"([0-9]+)", cold_rent)
        cold_rent = "".join(cold_rent)

        warm_rent = re.findall(r"([0-9]+)", warm_rent)
        warm_rent = "".join(warm_rent)
        
        cold_rent = int(cold_rent)
        warm_rent = int (warm_rent)
        if(warm_rent > cold_rent):
            rent = str(warm_rent)
        else: 
            rent = str(cold_rent)
        if(not re.search(r"([0-9]+)",rent)):
            return

        currency = "EUR"

        deposit = response.css("div.dt:contains('Kaution') + div.dd::text").get()
        if(deposit != None):
            deposit = deposit.split(",")[0]
            deposit = re.findall("([0-9]+)", deposit)
            deposit = "".join(deposit)

        heating_cost = response.css("div.dt:contains('Heizkosten') + div.dd::text").get()
        if(heating_cost != None):
            heating_cost = heating_cost.split(",")[0]
            heating_cost = re.findall("([0-9]+)", heating_cost)
            heating_cost = "".join(heating_cost)

        utilities = response.css("div.dt:contains('Nebenkosten') + div.dd::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
            utilities = re.findall("([0-9]+)", utilities)
            utilities = "".join(utilities)
        
        address = response.css("div.dt:contains('Adresse') + div.dd::text").getall()
        address = " ".join(address)
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])
        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("div.dt:contains('Objekt ID') + div.dd::text").get()
        floor = response.css("div.dt:contains('Etage') + div.dd::text").get()
        square_meters = response.css("div.dt:contains('Wohnfläche') + div.dd::text").get()
        square_meters = square_meters.split(",")[0]

        room_count = response.css("div.dt:contains('Zimmer') + div.dd::text").get()
        bathroom_count = response.css("div.dt:contains('Badezimmer') + div.dd::text").get()
        available_date = response.css("div.dt:contains('Verfügbar ab') + div.dd::text").get()

        amenities = response.css("li.list-group-item::text").getall()
        amenities = " ".join(amenities).strip()

        terrace = "Terrasse" in amenities
        balcony = "Balkon" in amenities

        energy_label = response.css("div.dt:contains('Energie­effizienz­klasse') + div.dd::text").get()
        
        images = response.css("div#immomakler-galleria a::attr(href)").getall()
        
        description = response.css("div.panel-body p::text").getall()
        description = " ".join(description)
        description = description_cleaner(description)

        landlord_name = "rennert-immobilien"
        landlord_phone = "+49(341) 490 900"
        landlord_email = "vermietung@rennert-immobilien.de"

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
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
