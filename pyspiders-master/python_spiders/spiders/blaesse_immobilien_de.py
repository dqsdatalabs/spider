# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import math

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, description_cleaner

class Blaesse_immobilien_deSpider(Spider):
    name = 'blaesse_immobilien_de'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.blaesse-immobilien.de"]
    start_urls = ["https://blaesse-immobilien.de/immobilien-stralsund/mieten-vermietung.html"]
    position = 1

    def parse(self, response):
        for url in response.css("div.immo_info a.immoleft::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)       

    def populate_item(self, response):
        
        property_type = "apartment"
        property_data = {}

        title = response.css("div.immo_item_detail h2 b::text").get()
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
            or "gastronomieobjekt" in lowered_title
        ):
            return

        cold_rent = response.css("td:contains('Kaltmiete:') + td::text").get()
        if(cold_rent):
            cold_rent = cold_rent.split(",")[0]

        warm_rent = response.css("td:contains('Warmmiete:') + td::text").get()
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
        
        
        utilities = response.css("td:contains('Nebenkosten') + td::text").get()
        if(utilities):
            utilities = utilities.split(",")[0]
        
        deposit = response.css("td:contains('Kaution') + td::text").get()
        if(deposit):
            deposit = deposit.split(",")[0]

        heating_cost = response.css("td:contains('Heizkosten') + td::text").get()
        if(heating_cost):
            heating_cost = heating_cost.split(",")[0]

        square_meters = response.css("td:contains('Wohnfläche:') + td::text").get()
        if(square_meters):
            square_meters = square_meters.split(",")[0]

        room_count = response.css("td:contains('Zimmer:') + td::text").get()
        try:
            room_count = re.findall("([1-9])", room_count)
            room_count = "".join(room_count)
        except:
            room_count = "1"
        
        if(not re.search(r"([1-9])", room_count)):
            room_count = "1"
        
        bathroom_count = response.css("td:contains('Badezimmer:') + td::text").get()
        floor = response.css("td:contains('Etage:') + td::text").get()
        available_date = response.css("td:contains('verfügbar ab:') + td::text").get()
        
        address = response.css("td a:contains('Deutschland')::text").get()
        if( not address):
            address = response.css("td a:contains('Groß')::text").get()
        if( not address):
            address = response.css("td a:contains('Garz')::text").get()

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        city = location_data[1]
        zipcode = location_data[0]

        description = response.css("div.openimmo_box_title + div.openimmo_box_form::text").getall()
        description = " ".join(description)

        description = description_cleaner(description)

        property_data["external_link"] = response.url
        property_data["property_type"] = property_type
        property_data["title"] = title
        property_data["rent"] = rent
        property_data["currency"] = currency
        property_data["utilities"] = utilities
        property_data["deposit"] = deposit
        property_data["heating_cost"] = heating_cost
        property_data["square_meters"] = square_meters
        property_data["room_count"] = room_count
        property_data["bathroom_count"] = bathroom_count
        property_data["floor"] = floor
        property_data["available_date"] = available_date
        property_data["address"] = address
        property_data["city"] = city
        property_data["zipcode"] = zipcode
        property_data["latitude"] = latitude
        property_data["longitude"] = longitude
        property_data["description"] = description

        images_url = response.css("a.tab:contains('Bilder')::attr(href)").get()
        yield Request(response.urljoin(images_url), callback=self.get_images, meta = {"property_data": property_data}, dont_filter = True)

    def get_images(self, response):
        property_data = response.meta.get("property_data")
        images = response.css("a#expose_imglink::attr(href)").getall()
        images = [ response.urljoin(image_src) for image_src in images]

        landlord_name = response.css("span#ea_agents_name::text").get()
        landlord_phone = response.css("p:contains('Telefon')::text").get()

        item_loader = ListingLoader(response=response)
        # # # # MetaData
        item_loader.add_value("external_link", property_data["external_link"]) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", property_data["title"]) # String
        item_loader.add_value("description", property_data["description"]) # String

        # # # Property Details
        item_loader.add_value("city", property_data["city"]) # String
        item_loader.add_value("zipcode", property_data["zipcode"]) # String
        item_loader.add_value("address", property_data["address"]) # String
        item_loader.add_value("latitude", property_data["latitude"]) # String
        item_loader.add_value("longitude", property_data["longitude"]) # String
        item_loader.add_value("floor", property_data["floor"]) # String
        item_loader.add_value("property_type", property_data["property_type"]) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", property_data["square_meters"]) # Int
        item_loader.add_value("room_count", property_data["room_count"]) # Int
        item_loader.add_value("bathroom_count", property_data["bathroom_count"]) # Int

        item_loader.add_value("available_date", property_data["available_date"]) # String => date_format

        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int

        item_loader.add_value("rent", property_data["rent"]) # Int
        item_loader.add_value("deposit", property_data["deposit"]) # Int
        item_loader.add_value("utilities", property_data["utilities"]) # Int
        item_loader.add_value("currency", property_data['currency']) # String

        item_loader.add_value("heating_cost", property_data["heating_cost"]) # Int

        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String

        yield item_loader.load_item()
